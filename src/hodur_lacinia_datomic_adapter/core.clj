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
    (:lacinia->datomic/eid param)     :eid
    (:lacinia->datomic/lookup param) :lookup
    :else                             :unknown))

[:employee/first-name
 :employee/last-name]

[:employee/first-name
 :employee/last-name
 {:employee/boss [:employee/full-name]}]

(defn ^:private extract-lacinia-selections
  [context]
  (-> context
      :com.walmartlabs.lacinia.constants/parsed-query
      :selections))

(defn ^:private find-selection-by-field
  [field-name selections]
  (->> selections
       (filter #(= field-name (:field %)))
       first))

(defn ^:private build-datomic-selector
  [field-selection engine-conn]
  (let [inner-selections (:selections field-selection)
        type-name (-> field-selection :field-definition :type :type :type)
        selector '[* {:field/parent [*]
                      :field/type [*]}]
        where [['?f :datomic/tag true]
               ['?f :field/parent '?tp]
               ['?tp :type/PascalCaseName type-name]] 
        query (concat '[:find ?f :where] where)
        eids (-> (q/q query @engine-conn)
                 vec flatten)
        fields (->> eids
                    (d/pull-many @engine-conn selector))]
    (reduce (fn [c selection] 
              (if-let [field (->> fields
                                  (filter #(= (:field selection)
                                              (:field/camelCaseName %)))
                                  first)]
                (let [field-name (:field/name field)
                      datomic-field-name (keyword
                                          (->kebab-case-string type-name)
                                          (->kebab-case-string field-name))]
                  (if (:selections selection)
                    (conj c (assoc {} datomic-field-name
                                   (build-datomic-selector selection engine-conn)))
                    (conj c datomic-field-name)))
                c))
            [] inner-selections)))

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

(defn ^:private field->resolve-pull-entry
  [{:keys [field/camelCaseName field/parent param/_parent] :as field}]
  (let [pull-param (first _parent)
        arg-type (pull-param-type pull-param)]
    {:field-name camelCaseName
     :arg-name (:param/camelCaseName pull-param)
     :arg-type arg-type
     :ident (if (= :lookup arg-type)
              (:lacinia->datomic/lookup pull-param)
              :none)}))

(defn attach-resolvers
  [lacinia-schema engine-conn db-conn]
  (->> engine-conn
       query-resolve-pull-fields
       (map field->resolve-pull-entry)
       (reduce
        (fn [m {:keys [field-name arg-name arg-type ident] :as field-entry}]
          (assoc-in m [:queries field-name :resolve]
                    (fn [ctx args resolved-value]
                      (let [field-selections (->> ctx
                                                  extract-lacinia-selections
                                                  (find-selection-by-field field-name))
                            selector (build-datomic-selector field-selections engine-conn)
                            arg-val (get args arg-name)]
                        ;;FIXME this is where it should connect to db-conn
                        (println {:selector selector
                                  :eid (cond
                                         (= :eid arg-type) arg-val
                                         (= :lookup arg-type) [ident arg-val])})))))
        lacinia-schema)))



(def s
  '[^{:datomic/tag-recursive {:except [full-name]}
      :lacinia/tag-recursive true}
    Employee
    [^String first-name
     ^String last-name
     ^{:type String
       :lacinia/resolve :employee/full-name-resolver}
     full-name
     ^{:type String
       :datomic/unique true}
     email
     ^Employee
     supervisor
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

     #_^{:type Employee
         :cardinality [0 n]
         :lacinia->datomic/type :query
         :lacinia/resolve :bla}
     employeesWithOffsetAndLimit
     #_[^{:type Integer
          :lacinia->datomic/offset true}
        offset
        ^{:type Integer
          :lacinia->datomic/limit true}
        limit]

     #_^{:type Employee
         :cardinality [0 n]
         :lacinia->datomic/type :query
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

(lacinia/execute compiled-schema
                 "{ A: employeeByEmail (email: \"foo\") { firstName lastName fullName }
                    employeeById (id: 3) { email fullName }
                    B: employeeByEmail (email: \"bla\") { email fullName supervisor { firstName fullName } } }"
                 nil nil)

#_(lacinia/execute compiled-schema
                   "{ B: employeeByEmail (email: \"bla\") { email fullName supervisor { firstName fullName } } }"
                   nil nil)

#_(lacinia/execute compiled-schema
                   "{ employeeByEmail (email: \"foo\") {firstName lastName fullName}}"
                   nil nil)
