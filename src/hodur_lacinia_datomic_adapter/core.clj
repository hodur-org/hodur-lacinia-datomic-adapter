(ns hodur-lacinia-datomic-adapter.core
  (:require [camel-snake-kebab.core :refer [->camelCaseKeyword
                                            ->PascalCaseKeyword
                                            ->PascalCaseString
                                            ->SCREAMING_SNAKE_CASE_KEYWORD
                                            ->kebab-case-keyword
                                            ->kebab-case-string]]
            [datomic.client.api :as datomic]
            [datascript.core :as d]
            [datascript.query-v3 :as q]
            ;;FIXME: these below are probably not needed
            [hodur-engine.core :as engine]
            [hodur-lacinia-schema.core :as hls]
            [hodur-datomic-schema.core :as hds]
            [com.walmartlabs.lacinia.util :as l-util]
            [com.walmartlabs.lacinia.schema :as l-schema]
            [com.walmartlabs.lacinia :as lacinia]))


(defn ^:private pull-param-type
  [param]
  (cond
    (:lacinia->datomic.param/eid param)    :eid
    (:lacinia->datomic.param/lookup param) :lookup
    :else                                  :unknown))

(defn ^:private extract-lacinia-selections
  [context]
  (-> context
      :com.walmartlabs.lacinia.constants/parsed-query
      :selections))

(defn ^:private extract-lacinia-type-name
  [{:keys [field-definition] :as selection}]
  (let [kind (-> field-definition :type :kind)]
    (case kind
      :list (-> field-definition :type :type :type :type)
      :non-null (-> field-definition :type :type :type)
      :root (-> field-definition :type :type))))

(defn ^:private find-selection-by-field
  [field-name selections]
  (->> selections
       (filter #(= field-name (:field %)))
       first))

(defn ^:private find-field-on-lacinia-field-name
  [lacinia-field-name fields]
  (->> fields
       (filter #(= lacinia-field-name
                   (:field/camelCaseName %)))
       first))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ^:private query-resolve-pull-fields
  [engine-conn]
  (let [selector '[* {:field/parent [*]
                      :param/_parent [*]}]
        eids (-> (q/q '[:find ?f
                        :where
                        [?f :lacinia->datomic.query/type :pull]
                        [?f :field/parent ?t]
                        [?t :lacinia/query true]
                        [?p :param/parent ?f]
                        (or [?p :lacinia->datomic.param/lookup]
                            [?p :lacinia->datomic.param/eid])]
                      @engine-conn)
                 vec flatten)]
    (->> eids
         (d/pull-many @engine-conn selector))))

(defn ^:private query-resolve-find-fields
  [engine-conn]
  (let [selector '[* {:field/parent [*]
                      :param/_parent [*]}]
        eids (-> (q/q '[:find ?f
                        :where
                        [?f :lacinia->datomic.query/type :find]
                        [?f :field/parent ?t]
                        [?t :lacinia/query true]
                        [?p :param/parent ?f]
                        (or [?p :lacinia->datomic.param/offset]
                            [?p :lacinia->datomic.param/limit])]
                      @engine-conn)
                 vec flatten)]
    (->> eids
         (d/pull-many @engine-conn selector))))

(defn ^:private query-datomic-fields-on-lacinia-type
  [lacinia-type-name engine-conn]
  ;;The reason I had to break into multiple queries is that datascript's `or
  ;;it returns a union instead  (it's prob a bug on `q)
  (let [selector '[* {:field/parent [*]
                      :field/type [*]}]
        where-datomic [['?f :field/parent '?tp]
                       ['?tp :type/PascalCaseName lacinia-type-name]
                       ['?f :datomic/tag true]]
        where-depends [['?f :field/parent '?tp]
                       ['?tp :type/PascalCaseName lacinia-type-name]
                       ['?f :lacinia->datomic.field/depends-on]]
        where-reverse [['?f :field/parent '?tp]
                       ['?tp :type/PascalCaseName lacinia-type-name]
                       ['?f :lacinia->datomic.field/reverse-lookup]]
        where-dbid [['?f :field/parent '?tp]
                    ['?tp :type/PascalCaseName lacinia-type-name]
                    ['?f :lacinia->datomic.field/dbid true]]

        query-datomic (concat '[:find ?f :where] where-datomic)
        query-depends (concat '[:find ?f :where] where-depends)
        query-reverse (concat '[:find ?f :where] where-reverse)
        query-dbid (concat '[:find ?f :where] where-dbid)

        eids-datomic (-> (q/q query-datomic @engine-conn)
                         vec flatten)
        eids-depends (-> (q/q query-depends @engine-conn)
                         vec flatten)
        eids-reverse (-> (q/q query-reverse @engine-conn)
                         vec flatten)
        eids-dbid (-> (q/q query-dbid @engine-conn)
                      vec flatten)

        eids (concat eids-datomic eids-depends
                     eids-reverse eids-dbid)]
    (->> eids
         (d/pull-many @engine-conn selector))))

(defn ^:private query-datomic-fields
  [engine-conn]
  (let [selector '[* {:field/parent [*]
                      :field/type [*]}]
        query '[:find ?f
                :where
                [?f :field/name]
                (or [?f :lacinia->datomic.field/dbid true]
                    [?f :datomic/tag true])]
        eids (-> (q/q query @engine-conn)
                 vec flatten)]
    (->> eids
         (d/pull-many @engine-conn selector))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ^:private lacinia->datomic-map
  [type-name fields]
  (reduce (fn [m {:keys [field/camelCaseName
                         field/kebab-case-name
                         lacinia->datomic.field/reverse-lookup
                         lacinia->datomic.field/depends-on
                         lacinia->datomic.field/dbid] :as field}]
            (let [datomic-field-name
                  (cond
                    dbid           [:db/id]
                    depends-on     depends-on
                    reverse-lookup [reverse-lookup]
                    :else          [(keyword
                                     (->kebab-case-string type-name)
                                     (name kebab-case-name))])]
              (assoc m camelCaseName datomic-field-name)))
          {} fields))

(defn ^:private datomic->lacinia-map
  [fields]
  (reduce (fn [m {:keys [field/camelCaseName
                         field/kebab-case-name
                         lacinia->datomic.field/reverse-lookup
                         lacinia->datomic.field/dbid] :as field}]
            (let [datomic-field-name
                  (cond
                    dbid           :db/id
                    reverse-lookup reverse-lookup
                    :else          (keyword
                                    (name (-> field :field/parent :type/kebab-case-name))
                                    (name kebab-case-name)))]
              (assoc m datomic-field-name camelCaseName)))
          {} fields))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ^:private build-datomic-selector
  [field-selection engine-conn]
  (let [inner-selections (:selections field-selection)
        type-name (extract-lacinia-type-name field-selection)
        fields (query-datomic-fields-on-lacinia-type type-name engine-conn)
        lacinia->datomic (lacinia->datomic-map type-name fields)]
    (vec
     (reduce
      (fn [c selection]
        (let [lacinia-field-name (:field selection)
              datomic-fields (get lacinia->datomic lacinia-field-name)
              selection-selections (:selections selection)]
          (if datomic-fields
            (if selection-selections
              (conj c (assoc {} (first datomic-fields)
                             (build-datomic-selector selection engine-conn)))
              (apply conj c datomic-fields)))))
      #{} inner-selections))))

(defn ^:private reduce-lacinia-response
  [datomic-obj datomic->lacinia]
  (reduce-kv (fn [m k v]
               (let [lacinia-field-name (get datomic->lacinia k)]
                 (if (map? v)
                   (assoc m lacinia-field-name
                          (reduce-lacinia-response v datomic->lacinia))
                   (assoc m lacinia-field-name v))))
             {} datomic-obj))

(defn ^:private build-lacinia-response
  [datomic-obj engine-conn]
  (let [fields (query-datomic-fields engine-conn)
        datomic->lacinia (datomic->lacinia-map fields)]
    (reduce-lacinia-response datomic-obj datomic->lacinia)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ^:private field->resolve-pull-entry
  [{:keys [field/camelCaseName field/parent param/_parent] :as field}]
  (let [pull-param (first _parent)
        arg-type (pull-param-type pull-param)]
    {:resolve-type :pull
     :field-name camelCaseName
     :args (reduce
            (fn [m param]
              (assoc m (:param/camelCaseName param)
                     {:type (pull-param-type param)
                      :ident (case (pull-param-type param)
                               :lookup (:lacinia->datomic.param/lookup param)
                               :none)
                      :transform (:lacinia->datomic.param/transform param)}))
            {} _parent)
     :arg-name (:param/camelCaseName pull-param)
     :arg-type arg-type
     :transform (:lacinia->datomic.param/transform pull-param)
     :ident (if (= :lookup arg-type)
              (:lacinia->datomic.param/lookup pull-param)
              :none)}))

(defn ^:private field->resolve-find-entry
  [{:keys [field/camelCaseName field/parent param/_parent] :as field}]
  ;;FIXME will needs something along these lines when :find is done
  (let [pull-param (first _parent)
        arg-type (pull-param-type pull-param)]
    {:resolve-type :find
     :field-name camelCaseName
     :arg-name (:param/camelCaseName pull-param)
     :arg-type arg-type
     :ident (if (= :lookup arg-type)
              (:lacinia->datomic.param/lookup pull-param)
              :none)}))

(defn ^:private build-pull-eid
  [inboud-args engine-args]
  (reduce-kv (fn [a in-arg-name in-arg-val]
               (let [{:keys [type ident transform]} (get engine-args in-arg-name)
                     arg-val (if transform
                               ((find-var transform) in-arg-val)
                               in-arg-val)]
                 (if type
                   (reduced (cond
                              (= :eid type) (Long/parseLong arg-val)
                              (= :lookup type) [ident arg-val])))))
             nil inboud-args))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn attach-resolvers
  [lacinia-schema engine-conn db-conn]
  (let [pull-fields (->> engine-conn
                         query-resolve-pull-fields
                         (map field->resolve-pull-entry))
        find-fields (->> engine-conn
                         query-resolve-find-fields
                         (map field->resolve-find-entry))]
    (->> (concat pull-fields find-fields)
         (reduce
          (fn [m {:keys [resolve-type field-name
                         arg-name arg-type ident transform] :as field-entry}]
            (cond-> m
              (= :pull resolve-type)
              (assoc-in [:queries field-name :resolve]
                        (fn [ctx args resolved-value]
                          (let [field-selection (->> ctx
                                                     extract-lacinia-selections
                                                     (find-selection-by-field field-name))
                                selector (build-datomic-selector field-selection engine-conn)
                                eid (build-pull-eid args (:args field-entry))
                                db (datomic/db db-conn)]
                            (println "Resolving pull for args" args)
                            (println "selector:")
                            (clojure.pprint/pprint selector)
                            (println "eid:")
                            (println eid)
                            (println "------")
                            (-> db
                                (datomic/pull selector eid)
                                (build-lacinia-response engine-conn)))))

              (= :find resolve-type)
              (assoc-in [:queries field-name :resolve]
                        (fn [ctx args resolved-value]
                          #_(let [field-selections (->> ctx
                                                        extract-lacinia-selections
                                                        (find-selection-by-field field-name))
                                  selector (build-datomic-selector field-selections engine-conn)
                                  arg-val (get args arg-name)]
                              ;;FIXME this is where it should connect to db-conn
                              (println {:selector selector
                                        :eid (cond
                                               (= :eid arg-type) arg-val
                                               (= :lookup arg-type) [ident arg-val])})))))
            )
          lacinia-schema))))

(defn transform-email
  [v]
  (println v)
  v)

(def s
  '[^{:datomic/tag-recursive {:except [id full-name reportees]}
      :lacinia/tag-recursive true}
    Employee
    [^{:type ID
       :lacinia->datomic.field/dbid true}
     id
     
     ^{:type String
       :datomic/unique :db.unique/identity}
     email

     ^String
     first-name

     ^String
     last-name

     ^{:type String
       :lacinia/resolve :employee/full-name-resolver
       :lacinia->datomic.field/depends-on [:employee/first-name
                                           :employee/last-name]}
     full-name
     
     ^{:type Employee
       :optional true}
     supervisor

     ^{:type Employee
       :cardinality [0 n]
       :lacinia->datomic.field/reverse-lookup :employee/_supervisor}
     reportees
     
     ^{:type Project
       :cardinality [0 n]}
     projects]

    ^{:datomic/tag-recursive true
      :lacinia/tag-recursive true}
    Project
    [^String name]

    ^{:datomic/tag-recursive true
      :lacinia/tag-recursive true
      :enum true}
    EmploymentType
    [FULL_TIME PART_TIME]
    
    ^{:lacinia/tag-recursive true
      :lacinia/query true}
    QueryRoot
    [^{:type Employee
       :lacinia->datomic.query/type :pull}
     employee
     [^{:type String
        :optional true
        :lacinia->datomic.param/lookup :employee/email
        :lacinia->datomic.param/transform hodur-lacinia-datomic-adapter.core/transform-email}
      email
      ^{:type ID
        :optional true
        :lacinia->datomic.param/eid true}
      id]

     ^{:type Employee
       :cardinality [0 n]
       :lacinia->datomic.query/type :find}
     employeesWithOffsetAndLimit
     [^{:type Integer
        :optional true
        :default 0 
        :lacinia->datomic.param/offset true}
      offset
      ^{:type Integer
        :optional true
        :default 50
        :lacinia->datomic.param/limit true}
      limit]

     #_^{:type Employee
         :cardinality [0 n]
         :lacinia->datomic.query/type :find
         :lacinia/resolve :bla}
     employeesWithFirstAfter
     #_[^{:type Integer
          :optional true
          :lacinia->datomic/first true}
        first
        ^{:type Integer
          :optional true
          :lacinia->datomic/last true}
        last
        ^{:type ID
          :optional true
          :lacinia->datomic/before true}
        before
        ^{:type ID
          :optional true
          :lacinia->datomic/after true}
        after]]])

(def conn (engine/init-schema s))

(def lacinia-schema (hls/schema conn))

(def datomic-schema (hds/schema conn))


(defn bla [context args resolved-value]
  #_(println "root" context args resolved-value)
  {:firstName "Fabiana"
   :lastName "Luchini"})

(defn full-name-resolver [context args {:keys [:firstName :lastName] :as resolved-value}]
  #_(println "field level" context args resolved-value)
  #_(clojure.pprint/pprint context)
  (str firstName " " lastName))

#_(attach-resolvers lacinia-schema conn nil)

(def cfg {:server-type :ion
          :region "us-east-2"
          :system "datomic-cloud-luchini"
          :query-group "datomic-cloud-luchini"
          :endpoint "http://entry.datomic-cloud-luchini.us-east-2.datomic.net:8182/"
          :proxy-port 8182})

(def client (datomic/client cfg))

(defn ^:private ensure-db [client db-name]
  (-> client
      (datomic/create-database {:db-name db-name}))
  (let [db-conn (datomic/connect client {:db-name db-name})]
    (datomic/transact db-conn {:tx-data datomic-schema})
    db-conn))

(def db-conn (-> client
                 (ensure-db "hodur-test")))

(datomic/transact db-conn {:tx-data [{:employee/email "tl@work.co"
                                      :employee/first-name "Tiago"
                                      :employee/last-name "Luchini"}
                                     {:employee/email "me@work.co"
                                      :employee/first-name "Marcelo"
                                      :employee/last-name "Eduardo"}
                                     {:employee/email "zeh@work.co"
                                      :employee/first-name "Zeh"
                                      :employee/last-name "Fernandes"
                                      :employee/supervisor [:employee/email "tl@work.co"]}]})

(def compiled-schema (-> lacinia-schema
                         (l-util/attach-resolvers
                          {:bla bla
                           :employee/full-name-resolver full-name-resolver})
                         (attach-resolvers conn db-conn)
                         l-schema/compile))



#_(lacinia/execute compiled-schema
                   "{ A: employeeByEmail (email: \"foo\") { firstName lastName fullName }
                    employeeById (id: 3) { email fullName }
                    B: employeeByEmail (email: \"bla\") { email fullName supervisor { firstName fullName } } }"
                   nil nil)

#_(lacinia/execute compiled-schema
                   "{ B: employeeByEmail (email: \"bla\") { email fullName supervisor { firstName fullName } } }"
                   nil nil)

#_(lacinia/execute compiled-schema
                   "{ employeeByEmail (email: \"foo\") { fullName firstName projects { name } supervisor { fullName } } }"
                   nil nil)

#_(lacinia/execute
   compiled-schema
   "{ employeeByEmail (email: \"zeh@work.co\") { fullName supervisor { firstName } reportees { lastName } } }"
   nil nil)

#_(lacinia/execute
   compiled-schema
   "{ employeesWithOffsetAndLimit { fullName supervisor { firstName } reportees { lastName } } }"
   nil nil)

#_(lacinia/execute
   compiled-schema
   "{ employeeByEmail (email: \"zeh@work.co\") { fullName firstName supervisor { fullName } } }"
   nil nil)


#_(lacinia/execute
   compiled-schema
   "{ employee (email: \"zeh@work.co\") { id fullName firstName supervisor { fullName } } }"
   nil nil)

(lacinia/execute
 compiled-schema
 "{ A:employee (id: \"42630264832131144\") { id fullName firstName supervisor { fullName } }
    B:employee (email: \"zeh@work.co\") { id fullName firstName supervisor { fullName } }}"
 nil nil
 )
