(ns core
  (:import (java.util UUID)))


;;

(def db {:eavt     {}
         :aevt     {}
         :last-tid 0})

(defn hash-datom
  [[e a v]]
  (str e a v))

(defn update-eavt
  [index action [eid attr value tid]]
  (case action
    :db/add
    (update-in index [eid] conj [eid attr value tid :assert])

    :db/retract
    (update-in index [eid] conj [eid attr value tid :retract])))

(defn update-aevt
  [index action [attr eid value tid]]
  (case action
    :db/add
    (update-in index [attr] conj [eid attr value tid :assert])

    :db/retract
    (update-in index [attr] conj [eid attr value tid :retract])))

(defn update-index
  [type index action datom]
  (case type
    :eavt (update-eavt index action datom)
    :aevt (update-aevt index action datom)))

(defn entity-by-id
  [{eavt :eavt} eid]
  (when-let [datoms (get eavt eid)]
    (loop [entity {}
           seen #{}
           [[_ attr value _ op :as datom] & datoms] datoms]
      (if-not attr
        (when-not (empty? entity)
          (assoc entity :db/id eid))

        (let [hash (hash-datom datom)]
          (if (seen hash)
            (recur entity seen datoms)
            (case op
              :assert
              (recur (assoc entity attr value) (conj seen hash) datoms)

              :retract
              (recur entity (conj seen hash) datoms))))))))

(defn datoms-by-eid
  [{eavt :eavt} eid]
  (get eavt eid))

(defn datoms-by-attr
  [{aevt :aevt} attr]
  (get aevt attr))

(defn all-datoms
  [db]
  (->> (vals (:eavt db))
       (apply concat)))

(defn data->actions
  [db data]
  (if (vector? data)
    (-> (for [[action eid :as all] data]
          (case action
            :db.fn/retractEntity
            (if-let [entity (entity-by-id db eid)]
              (for [[attr value] entity
                    :when (not= attr :db/id)]
                [:db/retract eid attr value])
              (throw (ex-info "entity not found" {:eid eid})))

            :db/retract
            [all]))
        first)

    (let [eid (or (:db/id data) (str (UUID/randomUUID)))
          entity (entity-by-id db eid)]
      (->>
        (for [[attr value] data
              :let [ovalue (get entity attr)]
              :when (and (not= attr :db/id)
                         (not= value ovalue))]
          (if ovalue
            [[:db/retract eid attr ovalue]
             [:db/add eid attr value]]

            [[:db/add eid attr value]]))
        (apply concat)))))

(defn transaction
  [{:keys [eavt aevt last-tid] :as db} data]
  (let [actions (data->actions db data)
        tid (inc last-tid)]

    (loop [eavt eavt
           aevt aevt
           [[action eid attr value] & actions] actions]
      (if-not action
        (assoc db :eavt eavt
                  :aevt aevt
                  :last-tid tid)

        (recur (update-index :eavt eavt action [eid attr value tid])
               (update-index :aevt aevt action [attr eid value tid])
               actions)))))

;; query

(defn is-binding-var
  [x]
  (.startsWith (name x) "?"))

(defn load-datoms
  [db [eid attr _]]
  (cond
    (not (is-binding-var eid))
    (datoms-by-eid db eid)

    (not (is-binding-var attr))
    (datoms-by-attr db attr)

    :else
    (all-datoms db)))

(defn match-datom
  [frame pattern datom]
  (reduce (fn [frame [i binding-var]]
            (let [binded-var (get frame binding-var binding-var)
                  datom-var (get datom i)]
              (if (is-binding-var binding-var)
                (assoc frame binding-var datom-var)

                (if (= datom-var binded-var)
                  frame
                  (reduced nil)))))
          frame (map-indexed vector pattern)))

(defn match-pattern
  [frame pattern datoms]
  (-> (reduce (fn [[frames seen] datom]
                (let [hash (str (first datom) (second datom))]
                  (if (seen hash)
                    [frames seen]
                    (if (= (last datom) :assert)
                      (if-let [frame (match-datom frame pattern datom)]
                        [(conj frames frame) (conj seen hash)]
                        [frames (conj seen hash)])
                      [frames (conj seen hash)]))))
              [[] #{}] datoms)
      first))

(defn unify
  [frame pattern]
  (for [v pattern]
    (get frame v v)))

(defn q
  [db {:keys [find where]}]
  (let [frames (reduce
                 (fn [frames pattern]
                   (mapcat #(match-pattern % pattern (load-datoms
                                                       db
                                                       (unify % pattern))) frames))
                 [{}] where)]
    (for [frame frames]
      (for [f find]
        (get frame f)))))

(comment
  (def db-after (transaction db {:name    "Gabriele"
                                 :surname "Carrettoni"}))
  (def db-after (transaction db-after {:name    "Gabriele"
                                       :surname "Cafarelli"}))
  (def db-after (transaction db-after {:name    "Marco"
                                       :surname "Carrettoni"}))

  (def entities (q db-after {:find  '[?eid]
                             :where '[[?eid :name "Gabriele"]]}))

  (map (fn [[eid]] (entity-by-id db-after eid)) entities)

  ;({:surname "Carrettoni", :name "Gabriele", :db/id "c58a34ee-243f-4f44-aa3a-01781dd2e73a"}
  ;  {:surname "Cafarelli", :name "Gabriele", :db/id "2849662f-256e-49a8-a464-d4ac7a601d37"})

  (def entities (q db-after {:find  '[?eid]
                             :where '[[?eid :name "Gabriele"]
                                      [?eid :surname "Cafarelli"]]}))

  (map (fn [[eid]] (entity-by-id db-after eid)) entities)

  ;; ({:surname "Cafarelli", :name "Gabriele", :db/id "2849662f-256e-49a8-a464-d4ac7a601d37"})

  (def db-after (transaction db-after {:name  "Giuseppe"
                                       :db/id (ffirst entities)}))

  (def entities (q db-after {:find  '[?eid]
                             :where '[[?eid :name "Gabriele"]]}))

  (map (fn [[eid]] (entity-by-id db-after eid)) entities)

  ;; ({:surname "Carrettoni", :name "Gabriele", :db/id "c58a34ee-243f-4f44-aa3a-01781dd2e73a"})

  (def db-after (transaction db-after [[:db.fn/retractEntity "c58a34ee-243f-4f44-aa3a-01781dd2e73a"]]))

  (def entities (q db-after {:find  '[?eid]
                             :where '[[?eid :name "Gabriele"]]}))

  (map (fn [[eid]] (entity-by-id db-after eid)) entities)

  ;; ()
  )