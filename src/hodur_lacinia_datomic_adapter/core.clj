(ns hodur-lacinia-datomic-adapter.core
  (:require [camel-snake-kebab.core :refer [->camelCaseKeyword
                                            ->PascalCaseKeyword
                                            ->PascalCaseString
                                            ->SCREAMING_SNAKE_CASE_KEYWORD
                                            ->kebab-case-keyword
                                            ->kebab-case-string]]
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
    (:lacinia->datomic/eid param)    :eid
    (:lacinia->datomic/lookup param) :lookup
    :else                            :unknown))

(defn ^:private extract-lacinia-selections
  [context]
  (-> context
      :com.walmartlabs.lacinia.constants/parsed-query
      :selections))

(defn ^:private extract-lacinia-type-name
  [{:keys [field-definition] :as selection}]
  (let [kind (-> field-definition :type :kind)]
    (if (= :list kind)
      (-> field-definition :type :type :type :type)
      (-> field-definition :type :type :type))))

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
                        [?f :lacinia->datomic/type :pull]
                        [?f :field/parent ?t]
                        [?t :lacinia/query true]
                        [?p :param/parent ?f]
                        (or [?p :lacinia->datomic/lookup]
                            [?p :lacinia->datomic/eid])]
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
                        [?f :lacinia->datomic/type :find]
                        [?f :field/parent ?t]
                        [?t :lacinia/query true]
                        [?p :param/parent ?f]
                        (or [?p :lacinia->datomic/offset]
                            [?p :lacinia->datomic/limit]
                            [?p :lacinia->datomic/first]
                            [?p :lacinia->datomic/last]
                            [?p :lacinia->datomic/after]
                            [?p :lacinia->datomic/before])]
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
                       ['?f :lacinia->datomic/depends-on]]
        where-reverse [['?f :field/parent '?tp]
                       ['?tp :type/PascalCaseName lacinia-type-name]
                       ['?f :lacinia->datomic/reverse-lookup]]
        query-datomic (concat '[:find ?f :where] where-datomic)
        query-depends (concat '[:find ?f :where] where-depends)
        query-reverse (concat '[:find ?f :where] where-reverse)
        eids-datomic (-> (q/q query-datomic @engine-conn)
                         vec flatten)
        eids-depends (-> (q/q query-depends @engine-conn)
                         vec flatten)
        eids-reverse (-> (q/q query-reverse @engine-conn)
                         vec flatten)
        eids (concat eids-datomic eids-depends eids-reverse)]
    (->> eids
         (d/pull-many @engine-conn selector))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ^:private build-datomic-selector
  [field-selection engine-conn]
  (let [inner-selections (:selections field-selection)
        type-name (extract-lacinia-type-name field-selection)
        fields (query-datomic-fields-on-lacinia-type type-name engine-conn)]
    (vec
     (reduce
      (fn [c selection]
        (if-let [field (find-field-on-lacinia-field-name
                        (:field selection) fields)]
          (let [field-name (:field/name field)
                reverse-loopkup (:lacinia->datomic/reverse-lookup field)
                datomic-field-name (if reverse-loopkup
                                     reverse-loopkup
                                     (keyword
                                      (->kebab-case-string type-name)
                                      (->kebab-case-string field-name)))
                selection-selections (:selections selection)
                depends-on (:lacinia->datomic/depends-on field)]
            (if depends-on
              (apply conj c depends-on)
              (if selection-selections
                (conj c (assoc {} datomic-field-name
                               (build-datomic-selector selection engine-conn)))
                (conj c datomic-field-name))))
          c))
      #{} inner-selections))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ^:private field->resolve-pull-entry
  [{:keys [field/camelCaseName field/parent param/_parent] :as field}]
  (let [pull-param (first _parent)
        arg-type (pull-param-type pull-param)]
    {:resolve-type :pull
     :field-name camelCaseName
     :arg-name (:param/camelCaseName pull-param)
     :arg-type arg-type
     :ident (if (= :lookup arg-type)
              (:lacinia->datomic/lookup pull-param)
              :none)}))

(defn ^:private field->resolve-find-entry
  [{:keys [field/camelCaseName field/parent param/_parent] :as field}]
  (let [pull-param (first _parent)
        arg-type (pull-param-type pull-param)]
    {:resolve-type :find
     :field-name camelCaseName
     :arg-name (:param/camelCaseName pull-param)
     :arg-type arg-type
     :ident (if (= :lookup arg-type)
              (:lacinia->datomic/lookup pull-param)
              :none)}))

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
                         arg-name arg-type ident] :as field-entry}]
            (cond-> m
              (= :pull resolve-type)
              (assoc-in [:queries field-name :resolve]
                        (fn [ctx args resolved-value]
                          (let [field-selections (->> ctx
                                                      extract-lacinia-selections
                                                      (find-selection-by-field field-name))
                                selector (build-datomic-selector field-selections engine-conn)
                                arg-val (get args arg-name)]
                            ;;FIXME this is where it should connect to db-conn
                            (clojure.pprint/pprint
                             {:selector selector
                              :eid (cond
                                     (= :eid arg-type) arg-val
                                     (= :lookup arg-type) [ident arg-val])}))))

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

(def s
  '[^{:datomic/tag-recursive {:except [full-name reportees]}
      :lacinia/tag-recursive true}
    Employee
    [^String first-name
     ^String last-name

     ^{:type String
       :lacinia/resolve :employee/full-name-resolver
       :lacinia->datomic/depends-on [:employee/first-name
                                     :employee/last-name]}
     full-name

     ^{:type String
       :datomic/unique true}
     email
     
     ^Employee
     supervisor

     ^{:type Employee
       :cardinality [0 n]
       :lacinia->datomic/reverse-lookup :employee/_supervisor}
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
       :lacinia->datomic/type :pull}
     employeeByEmail
     [^{:type String
        :lacinia->datomic/lookup :employee/email}
      email]

     ^{:type Employee
       :lacinia->datomic/type :pull}
     employeeById
     [^{:type Integer
        :lacinia->datomic/eid true}
      id]

     ^{:type Employee
       :cardinality [0 n]
       :lacinia->datomic/type :find}
     employeesWithOffsetAndLimit
     [^{:type Integer
        :lacinia->datomic/offset true}
      offset
      ^{:type Integer
        :lacinia->datomic/limit true}
      limit]

     #_^{:type Employee
         :cardinality [0 n]
         :lacinia->datomic/type :find
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

(def compiled-schema (-> lacinia-schema
                         (l-util/attach-resolvers
                          {:bla bla
                           :employee/full-name-resolver full-name-resolver})
                         (attach-resolvers conn nil)
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

(lacinia/execute compiled-schema
                 "{ employeeByEmail (email: \"foo\") { fullName supervisor { firstName } reportees { lastName } } }"
                 nil nil)
