(ns core
  (:import (java.util UUID)))

(def db {:eavt     {}
         :aevt     {}
         :last-tid 0})

(defn update-eavt
  [index action [eid attr value tid]]
  (case action
    :db/add
    (update-in index [eid] (fnil conj []) [attr value tid :assert])

    :db/retract
    (update-in index [eid] (fnil conj []) [attr value tid :retract])))

(defn update-aevt
  [index action [attr eid value tid]]
  (case action
    :db/add
    (update-in index [attr] (fnil conj []) [eid value tid :assert])

    :db/retract
    (update-in index [attr] (fnil conj []) [eid value tid :retract])))

(defn update-index
  [type index action datom]
  (case type
    :eavt (update-eavt index action datom)
    :aevt (update-aevt index action datom)))

(defn entity-by-id
  [{eavt :eavt} eid]
  (when-let [datoms (get eavt eid)]
    (loop [entity {}
           [[attr value _ op] & datoms] datoms]
      (if-not attr
        entity

        (case op
          :assert
          (recur (assoc entity attr value)
                 datoms)

          :retract
          (recur (dissoc entity attr value)
                 datoms))))))

(defn data->actions
  [db data]
  (if (vector? data)
    (-> (for [[action eid :as all] data]
          (case action
            :db.fn/retractEntity
            (if-let [entity (entity-by-id db eid)]
              (for [[attr value] entity]
                [:db/retract eid attr value])
              (throw (ex-info "entity not found" {:eid eid})))

            :db/retract
            [all]))
        first)

    (let [eid (or (:db/id data) (UUID/randomUUID))
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