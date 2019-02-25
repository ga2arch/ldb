(ns ldb.db
  (:require [clojure.data.fressian :as fress]
            [clojure.spec.alpha :as s])
  (:import (org.lmdbjava Env DbiFlags PutFlags KeyRange CursorIterator Dbi Txn)
           (java.io File)
           (java.nio ByteBuffer ByteOrder)
           (clojure.lang IReduceInit Named)
           (java.util UUID HashMap)))

(defrecord Connection [^Dbi eavt-current ^Dbi eavt-history
                       ^Dbi aevt-current ^Dbi aevt-history
                       ^Dbi avet-current ^Dbi avet-history
                       ^Dbi vaet-current ^Dbi vaet-history
                       ^Env env ^Dbi status ^Dbi ident])

;; specs

(s/def :db/ident keyword?)
(s/def :db/id (s/or :id int?
                    :tempid string?))
(s/def :db/valueType #{:db.type/string :db.type/keyword :db.type/long :db.type/ref})
(s/def :db/cardinality #{:db.cardinality/one :db.cardinality/many})
(s/def ::schema (s/keys :req [:db/ident :db/valueType :db/cardinality]))

;; const

(def schema-idents [:db/ident :db/valueType :db/cardinality
                    :db.type/keyword :db.type/string :db.type/long :db.type/ref
                    :db.cardinality/one :db.cardinality/many])

;;

(defn encode-key
  [data]
  (if (int? data)
    (let [bkey (ByteBuffer/allocateDirect 8)]
      (.order bkey (ByteOrder/BIG_ENDIAN))
      (.putInt bkey data)
      (.flip bkey))
    (let [data (fress/write data)
          bkey (ByteBuffer/allocateDirect (.limit data))]
      (.order bkey (ByteOrder/BIG_ENDIAN))
      (.put bkey data)
      (.flip bkey))))

(defn encode-val
  [data]
  (let [data (fress/write data)
        bkey (ByteBuffer/allocateDirect (.limit data))]
    (.order bkey (ByteOrder/BIG_ENDIAN))
    (.put bkey data)
    (.flip bkey)))

(defn decode
  [data]
  (fress/read data))

(defn decode-key
  [buffer int-key?]
  (if int-key?
    (let [n (.getInt buffer)]
      (.flip buffer)
      n)
    (decode buffer)))

(defn txn-read
  [^Connection conn]
  (.txnRead ^Env (.-env conn)))

(defn txn-write
  [^Connection conn]
  (.txnWrite ^Env (.-env conn)))

(defn txn-commit
  [^Txn txn]
  (.commit txn))

(defn put-key
  [^Dbi db ^Txn txn key val]
  (.put db txn (encode-key key) (encode-val val) (into-array PutFlags [])))

(defn get-key
  [^Dbi db ^Txn txn key]
  (when-let [v (.get db txn (encode-key key))]
    (decode v)))

(defn scan
  [^Dbi db ^Txn txn ^KeyRange range]
  (reify IReduceInit
    (reduce [this f init]
      (let [^CursorIterator it (.iterate db txn range)]
        (try
          (loop [state init]
            (if (reduced? state)
              @state
              (if (.hasNext it)
                (recur (f state (.next it)))
                state)))
          (finally
            (.close it)))))))

(defn scan-key
  [^Dbi db ^Txn txn key]
  (let [ekey (encode-key key)
        range (KeyRange/closed ekey ekey)]
    (eduction (map #(decode (.val %))) (scan db txn range))))

(defn scan-all
  ([^Dbi db ^Txn txn]
   (scan-all db txn false))
  ([^Dbi db ^Txn txn int-key?]
   (letfn [(rf [kv]
             (let [k (decode-key (.key kv) int-key?)
                   v (decode (.val kv))]
               [k v]))]
     (eduction (map rf) (scan db txn (KeyRange/all))))))

(defn del-key
  [^Dbi db ^Txn txn key]
  (.delete db txn (encode-key key)))

(defn del-kv
  [^Dbi db ^Txn txn key val]
  (.delete db txn (encode-key key) (encode-val val)))

(defn close
  [^Connection conn]
  (.close ^Env (.-env conn)))

(defn insert-ident
  [^Connection conn ^Txn txn key value]
  (put-key (.-ident conn) txn key value)
  (put-key (.-ident conn) txn (str value) key))

(defn from-ident
  [^Connection conn ^Txn txn key]
  (get-key (.-ident conn) txn (str key)))

(defn to-ident
  [^Connection conn ^Txn txn key]
  (get-key (.-ident conn) txn key))

(defn connect
  [^String filepath]
  (let [^Env env (-> (Env/create)
                     (.setMapSize 1099511627776)
                     (.setMaxDbs 7)
                     (.open (File. filepath) nil))]
    (letfn [(open-db [name & flags]
              (.openDbi env name (into-array DbiFlags flags)))]
      (let [conn (map->Connection {:env          env
                                   :eavt-current (open-db "eavt-current" DbiFlags/MDB_CREATE DbiFlags/MDB_DUPSORT)
                                   :eavt-history (open-db "eavt-history" DbiFlags/MDB_CREATE DbiFlags/MDB_DUPSORT)
                                   :aevt-current (open-db "aevt-current" DbiFlags/MDB_CREATE DbiFlags/MDB_DUPSORT)
                                   :aevt-history (open-db "aevt-history" DbiFlags/MDB_CREATE DbiFlags/MDB_DUPSORT)
                                   :status       (open-db "status" DbiFlags/MDB_CREATE)
                                   :ident        (open-db "ident" DbiFlags/MDB_CREATE)})]
        (with-open [txn (txn-write conn)]
          (doseq [[i ident] (map-indexed vector schema-idents)]
            (insert-ident conn txn ident (- (inc i))))
          (txn-commit txn))
        conn))))
;;

(defn ^:dynamic entity-by-id
  ([^Connection conn eid]
   (when eid
     (with-open [txn (txn-read conn)]
       (entity-by-id conn txn eid))))
  ([^Connection conn ^Txn txn eid]
   (when eid
     (let [xf (map (fn [[_ attr value _ _]] [(from-ident conn txn attr) value]))
           entity (into {} xf (scan-key (.-eavt-current conn) txn eid))]
       (when-not (empty? entity)
         (assoc entity :db/id eid))))))

(defn entity-by-ids
  [^Connection conn ^Txn txn eids]
  (eduction (map (partial entity-by-id conn txn))
            (filter (complement nil?)) eids))

(defn get-and-inc
  [^Connection conn ^Txn txn key]
  (let [eid (or (get-key (.-status conn) txn key) -1)]
    (put-key (.-status conn) txn key (inc eid))
    (inc eid)))

(defn valid-type?
  [valueType value]
  (case valueType
    :db.type/long
    (int? value)

    :db.type/string
    (string? value)

    :db.type/keyword
    (keyword? value)

    :db.type/ref
    (or (map? value)
        (int? value))

    false))

(defn valid-value?
  [{:db/keys [_ valueType cardinality]} value]
  (case cardinality
    :db.cardinality/one
    (valid-type? valueType value)

    :db.cardinality/many
    (and (coll? value)
         (every? (partial valid-type? valueType) value))))

(defn validate-value
  [value schema]
  (when-not (valid-value? schema value)
    (throw (ex-info "invalid value" {:value  value
                                     :schema schema}))))

(defn find-datom
  [^Connection conn ^Txn txn [eid attr value]]
  (let [xf (comp
             (filter (fn [[ceid cattr cvalue]] (and (= ceid eid) (= cattr attr) (= cvalue value))))
             (halt-when any?))]
    (transduce xf identity nil (scan-key (.-eavt-current conn) txn eid))))

(defn ^:dynamic gen-tempid [])

(defn map->actions
  [^Connection conn ^Txn txn m]
  (when (:db/ident m)
    (when-let [ex (s/explain-data ::schema m)]
      (throw (ex-info "invalid schema" ex))))

  (let [get-eid (fn [m] (or (:db/id m)
                            (to-ident conn txn (:db/ident m))
                            (gen-tempid)))
        eid (get-eid m)
        xf (fn [[attr value]]
             (cond
               (map? value)
               (let [new-eid (get-eid value)
                     datom [eid attr new-eid true]
                     value (assoc value :db/id new-eid)]
                 (eduction cat [[datom] (map->actions conn txn value)]))

               (and (coll? value)
                    (map? (first value)))
               (let [new-eids (mapv get-eid value)
                     datom [eid attr (set new-eids) true]
                     values (mapcat (fn [value neid]
                                      (map->actions conn txn (assoc value :db/id neid))) value new-eids)]
                 (eduction cat [[datom] values]))

               :else
               [[eid attr value true]]))]
    (eduction (mapcat xf) m)))

(defn action->datoms
  [^Connection conn ^Txn txn ^Integer tid tempids]
  (letfn [(tempid->eid [tempid]
            (if-let [id (.get tempids tempid)]
              id
              (let [eid (get-and-inc conn txn :last-eid)]
                (.put tempids tempid eid)
                eid)))

          (load-schema [attr]
            (entity-by-id conn txn (to-ident conn txn attr)))

          (resolve-tempids [attr value]
            (let [{:db/keys [valueType cardinality]} (load-schema attr)]
              (case valueType
                :db.type/ref
                (case cardinality
                  :db.cardinality/one
                  (if (string? value) (tempid->eid value) value)

                  :db.cardinality/many
                  (into #{} (map (fn [id] (if (string? id) (tempid->eid id) id)) value)))
                value)))

          (hydrate [[eid attr value tx op]]
            (let [value (resolve-tempids attr value)]
              (cond
                (int? eid)
                (if (entity-by-id conn txn eid)
                  [eid attr value tx op]
                  (throw (ex-info "entity not found" {:eid eid})))

                (string? eid)
                [(tempid->eid eid) attr value tx op]

                :default
                [(get-and-inc conn txn :last-eid) attr value tx op])))

          (filter-attr [[eid attr value]]
            (and (not= attr :db/id)
                 (not= value (get (entity-by-id conn txn eid) attr))))

          (update-ident [[eid attr value op]]
            (when (= :db/ident attr)
              (insert-ident conn txn value eid))
            (if-let [ident (to-ident conn txn attr)]
              [eid ident attr value op]
              (throw (ex-info "ident not found" {:attr  attr
                                                 :value value}))))

          (validate [[_ ident _ value :as all]]
            (when-not (neg? ident)
              (let [schema (entity-by-id conn txn ident)]
                (validate-value value schema)))
            all)

          (->datoms [[eid ident attr value op]]
            (if (neg? ident)
              (if-let [old-value (get (entity-by-id conn txn eid) attr)]
                [[eid ident old-value tid false]
                 [eid ident value tid true]]
                [[eid ident value tid true]])

              (let [value (if (coll? value) (set value) value)
                    old-value (get (entity-by-id conn txn eid) attr)
                    {:db/keys [cardinality]} (entity-by-id conn txn ident)]
                (case cardinality
                  :db.cardinality/one
                  (if (and op old-value)
                    [[eid ident old-value tid false]
                     [eid ident value tid true]]
                    [[eid ident value tid true]])

                  :db.cardinality/many
                  (if (and op old-value)
                    [[eid ident old-value tid false]
                     [eid ident (into value old-value) tid true]]
                    [[eid ident value tid true]])))))]

    (comp
      (map hydrate)
      (filter filter-attr)
      (map update-ident)
      (map validate)
      (mapcat ->datoms))))

(defn vector->actions
  [conn txn tid [action eid attr value]]
  (case action
    :db.fn/retractEntity
    (if-let [entity (entity-by-id conn txn eid)]
      (eduction (filter (fn [[attr value]] [eid attr value tid false])) entity)
      (throw (ex-info "entity not found" {:eid eid})))

    :db/add
    [[eid attr value true]]

    :db/retract
    [[eid attr value false]]))

(defn update-indexes
  [^Connection conn ^Txn txn [eid attr _ _ op :as datom]]
  (if op
    (do
      (put-key (.-eavt-current conn) txn eid datom)
      (put-key (.-aevt-current conn) txn attr datom)
      [datom])

    (let [[reid rattr :as to-retract] (find-datom conn txn datom)]
      (put-key (.-eavt-history conn) txn eid datom)
      (put-key (.-aevt-history conn) txn attr datom)

      (put-key (.-eavt-history conn) txn eid to-retract)
      (put-key (.-aevt-history conn) txn attr to-retract)

      (del-kv (.-eavt-current conn) txn reid to-retract)
      (del-kv (.-aevt-current conn) txn rattr to-retract)
      [datom to-retract])))

(defn insert->datoms
  [^Connection conn ^Txn txn ^Integer tid tx-data]
  (letfn [(step [state input]
            (let [xs (cond
                       (map? input)
                       (map->actions conn txn input)

                       (vector? input)
                       (vector->actions conn txn tid input))

                  datoms (into (:datoms state)
                               (comp
                                 (action->datoms conn txn tid (:tempids state))
                                 (mapcat (partial update-indexes conn txn)))
                               xs)]
              {:tempids (:tempids state)
               :datoms  datoms}))]

    (reduce step {:tempids (HashMap.)
                  :datoms  []} tx-data)))

(defn transact
  [^Connection conn data]
  (let [tx-data (:tx-data data)]
    (with-open [txn (txn-write conn)]
      (let [tid (System/currentTimeMillis)]
        (binding [entity-by-id (memoize entity-by-id)
                  gen-tempid (let [counter (volatile! 0)]
                               (fn []
                                 (str (vswap! counter inc))))]
          (let [result (insert->datoms conn txn tid tx-data)]
            (txn-commit txn)
            result))))))

(defn show-db
  [{:keys [eavt-current eavt-history aevt-current aevt-history status ident] :as conn}]
  (with-open [txn (txn-read conn)]
    (letfn [(load-index [db]
              (into (sorted-map) (group-by first (eduction (map second) (scan-all db txn)))))]
      {:eavt   {:current (load-index eavt-current)
                :history (load-index eavt-history)}
       :aevt   {:current (load-index aevt-current)
                :history (load-index aevt-history)}
       :status (into [] (scan-all status txn))
       :ident  (into [] (scan-all ident txn))})))

;; query

(defn binding-var?
  [x]
  (when (instance? Named x)
    (.startsWith (name x) "?")))

(defn load-datoms
  [^Connection conn ^Txn txn [eid attr val]]
  (let [filter-attr (filter (fn [[_ dattr _]] (= dattr attr)))
        filter-val (filter (fn [[_ _ dval]] (= dval val)))]
    (cond
      (not (binding-var? eid))
      (cond
        (and (not (binding-var? attr))
             (not (binding-var? val)))
        (eduction filter-attr
                  filter-val
                  (scan-key (.-eavt-current conn) txn eid))

        (not (binding-var? attr))
        (eduction filter-attr
                  (scan-key (.-eavt-current conn) txn eid))

        (not (binding-var? val))
        (eduction filter-val
                  (scan-key (.-eavt-current conn) txn eid))

        :else
        (scan-key (.-eavt-current conn) txn eid))

      (not (binding-var? attr))
      (if-not (binding-var? val)
        (eduction filter-attr
                  (scan-key (.-aevt-current conn) txn attr))
        (scan-key (.-aevt-current conn) txn attr))

      :else
      (if-not (binding-var? val)
        (eduction
          (mapcat second)
          filter-val
          (scan-all (.-aevt-current conn) txn))

        (eduction
          (mapcat second)
          (scan-all (.-aevt-current conn) txn))))))

(defn match-datom
  [frame pattern datom]
  (loop [i 0
         frame frame]
    (if (= i (count pattern))
      frame
      (let [binding-var (nth pattern i)
            binded-value (get frame binding-var binding-var)
            datom-value (nth datom i)]
        (if (binding-var? binding-var)
          (recur (inc i) (assoc frame binding-var datom-value))
          (when (= datom-value binded-value)
            (recur (inc i) frame)))))))

(defn match-pattern
  [frame pattern]
  (comp
    (map (partial match-datom frame pattern))
    (filter (complement nil?))))

(defn update-pattern
  [conn txn frame [eid attr value]]
  (letfn [(unify [var] (get frame var var))]
    [(unify eid) (to-ident conn txn (unify attr)) (unify value)]))

(defn entities-by-id [conn eids]
  (map (fn [[eid]] (entity-by-id conn eid)) eids))

(defn q
  [^Connection conn {:keys [find where]}]
  (with-open [txn (txn-read conn)]
    (letfn [(rf [pattern]
              (mapcat (fn [frame]
                        (let [pattern (update-pattern conn txn frame pattern)
                              datoms (load-datoms conn txn pattern)]
                          (eduction
                            (filter (fn [[_ _ _ _ op]] op))
                            (match-pattern frame pattern) datoms)))))
            (substitute [frame]
              (mapv (partial get frame) find))]
      (let [frames (eduction (apply comp (mapv rf where)) [{}])]
        (mapv substitute frames)))))