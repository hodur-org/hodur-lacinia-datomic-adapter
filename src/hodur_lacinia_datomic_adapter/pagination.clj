(ns hodur-lacinia-datomic-adapter.pagination
  (:require [datomic.client.api :as datomic]
            [datascript.core :as d]
            [datascript.query-v3 :as q]
            [hodur-engine.core :as engine]))

(def ^:private page-info
  '[^{:lacinia/tag-recursive true}
    PageInfo
    [^{:type Integer}
     total-pages

     ^{:type Integer}
     current-page

     ^{:type Integer}
     page-size
     
     ^{:type Integer}
     current-offset

     ^{:type Integer}
     next-offset

     ^{:type Integer}
     prev-offset]])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ^:private list-type-name
  [t]
  (str (:type/name t) "List"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ^:private query-relevant-types
  [conn]
  (let [selector [:type/name]
        eids
        (-> (q/q '[:find ?t
                   :where
                   [?t :datomic/tag true]
                   [?t :lacinia/tag true]
                   [?t :type/name]]
                 @conn)
            vec flatten)]
    (d/pull-many @conn selector eids)))

(defn ^:private query-cardinality-fields
  [conn]
  (let [selector [:field/name :field/cardinality
                  {:field/parent [:type/name]}]
        eids
        (-> (q/q '[:find ?f
                   :where
                   [?f :datomic/tag true]
                   [?f :lacinia/tag true]
                   [?f :field/name]
                   [?f :field/cardinality]]
                 @conn)
            vec flatten)]
    (d/pull-many @conn selector eids)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ^:private build-list-types
  [types]
  (reduce (fn [c t]
            (let [add-name (list-type-name t)]
              (-> c
                  (conj (vary-meta (symbol add-name) assoc :lacinia/tag-recursive true))
                  (conj `[^{:type Integer}
                          ~'total-count
                          ^{:type PageInfo}
                          ~'page-info
                          ^{:type ~(symbol (:type/name t))
                            :cardinality [0 ~'n]}
                          ~'nodes]))))
          [] types))

(defn ^:private build-list-fields
  [fields]
  (reduce (fn [c field]
            (println field)
            (let [field-name (-> field :field/name)
                  parent (-> field :field/parent)
                  type-name (-> parent :type/name)
                  list-name (-> parent list-type-name)]
              (println field-name)
              (-> c
                  (conj (vary-meta (symbol type-name) assoc :lacinia/tag-recursive true))
                  (conj `[^{:type ~list-name}
                          ~(symbol field-name)]))))
          [] fields))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn extend-schema
  [conn]
  (let [types (query-relevant-types conn)
        fields (query-cardinality-fields conn)]
    (->> types
         build-list-types 
         (concat page-info)
         (concat (build-list-fields fields))
         vec
         #_(engine/init-schema))
    
    #_conn))



(def s
  '[^{:datomic/tag-recursive true
      :lacinia/tag-recursive true}
    Person
    [^String name
     ^Person mum
     ^{:type Person
       :cardinality [0 n]}
     friends]

    ^{:datomic/tag-recursive true
      :lacinia/tag-recursive true}
    Pet
    [^String name]])


(def target
  '[^{:datomic/tag-recursive true
      :lacinia/tag-recursive true}
    Person
    [^String name
     ^Person mum
     ^{:type PersonList}
     friends]

    ^{:datomic/tag-recursive true
      :lacinia/tag-recursive true}
    Pet
    [^String name]])

(def eng-conn (engine/init-schema s))

(def extension (extend-schema eng-conn))

(def new-conn (engine/extend-schema eng-conn extension))

#_(clojure.pprint/pprint
   (d/q '[:find [(pull ?e [* {:field/_parent [*]}]) ...]
          :where
          [?e :datomic/tag true]]
        @new-conn))


(clojure.pprint/pprint
 (map (fn [i]
        {:name (:type/name i)
         :fields (map #(:field/name %) (:field/_parent i))})
      (d/q '[:find [(pull ?e [* {:field/_parent [*]}]) ...]
             :where
             [?e :type/name]
             [?e :lacinia/tag true]]
           @new-conn))
 )
