(ns crypticbutter.zmq.impl.pipe
  (:require
   [taoensso.encore :as enc]
   [crypticbutter.zmq.impl.pipe.ypipe :as ypipe]
   [crypticbutter.zmq.impl.mailbox :as mbox]
   [crypticbutter.zmq.impl.cmd :as cmd :refer [Commandable]]
   [crypticbutter.zmq.impl.msg :as msg])
  (:import
   (crypticbutter.zmq.impl.mailbox Mailbox)
   (crypticbutter.zmq.impl.msg Msg)
   (crypticbutter.zmq.impl.pipe.ypipe YPipe)))

(defprotocol IsBlob
  (size [_]))

(deftype Blob
         [^:bytes buffer]
  IsBlob
  (size [_] (.-length buffer))

  Object
  (equals [_ other]
    (if (instance? Blob other)
      (java.util.Arrays/equals buffer (.-buffer ^Blob other))
      false))
  (hashCode [_]
    (java.util.Arrays/hashCode buffer)))

(defn create-blob
  ([data] (if (instance? Msg data)
            (create-blob (msg/data ^Msg data) true)
            (create-blob data false)))
  ([data copy?]
   (if copy?
     (let [b (make-array Byte (.-length data))]
       (System/arraycopy data 0 b 0 (.-length data))
       (->Blob b))
     (->Blob data))))

(defprotocol PipeEventSink
  (activate-write! [_ pipe])
  (activate-read! [_ pipe])
  (hiccup! [_ pipe])
  (drop-pipe! [_ pipe] "Called when a pipe is terminated"))

(defprotocol PipeProtocol
  (set-event-sink! [_ sink])
  (set-peer! [_ peer-pipe mailbox])
  (set-id! [_ id])
  (get-id [_])
  (set-routing-id! [_ id])
  (get-routing-id [_])
  (set-no-term-delay! [_])
  (get-credential [_])
  (set-disconnect-msg! [_])
  (send-disconnect-msg! [_])
  (send-hiccup-msg! [_ hiccup-msg])
  (readable? [_])
  (read! [_])
  (writable? [_])
  (write! [_ msg])
  (flush! [_])
  (rollback! [_])
  (hiccup! [_])
  (terminate! [_ delay?])
  (-within-out-limit? [_])
  (-process-activate-read [_])
  (-process-activate-write [_ msg-read-count])
  (-process-delimiter! [_])
  (-process-hiccup [_ ypipe])
  (-process-pipe-term [_])
  (-process-pipe-term-ack! [_]))

(defprotocol PipeEventsProtocol
  (dispatch-read-activated [_ pipe])
  (dispatch-write-activated [_ pipe])
  (dispatch-hiccuped [_ pipe])
  (dispatch-pipe-terminated [_ pipe]))

(defn- hwm->lwm [hwm]
  (/ (inc hwm) 2))

(def ^:const stage-active 0)
(def ^:const stage-delim-received 1)
(def ^:const stage-awaiting-delim 2)
(def ^:const stage-term-ack-sent 3)
(def ^:const stage-term-req-sent 4)
(def ^:const stage-term-req-sent-peer 5)

(def msg-pipe-granularity 256)

(deftype Pipe
  [^:unsynchronized-mutable ^YPipe in-pipe
   ^:unsynchronized-mutable ^:boolean in-active?
   ^:int in-lower-limit ;; LWM
   ^:unsynchronized-mutable ^YPipe out-pipe
   ^:unsynchronized-mutable ^:boolean out-active?
   ^:int out-limit ;; HWM
   ^:unsynchronized-mutable ^:int msg-read-count
   ^:unsynchronized-mutable ^:int msg-write-count
   ^:unsynchronized-mutable ^:int peer-msg-read-count
   ^:unsynchronized-mutable peer-pipe
   ^:unsynchronized-mutable ^Mailbox peer-mbox
   ^:unsynchronized-mutable sink
   ^:unsynchronized-mutable ^:int stage
   ^:unsynchronized-mutable ^:boolean recv-msgs-before-term?
   ^:unsynchronized-mutable id
   ^:unsynchronized-mutable ^:int routing-id
   ^:unsynchronized-mutable credential
   conflate?
   ^:unsynchronized-mutable disconnect-msg]

  PipeProtocol
  (set-peer! [_ peer mbox]
    {:pre [(some? peer)]}
    (set! peer-pipe peer)
    (set! peer-mbox mbox))
  (set-event-sink! [_ new-sink] (set! sink new-sink))
  (set-id! [_ new-id] (set! id new-id))
  (get-id [_] id)
  (set-routing-id! [_ new-id] (set! routing-id new-id))
  (get-routing-id [_] routing-id)
  (get-credential [_] credential)
  (set-no-term-delay! [_]
    (set! recv-msgs-before-term? false))
  (rollback! [_]
    (when (some? out-pipe)
      (loop [more? true]
        (when more?
          (recur (ypipe/pop-incomplete! out-pipe))))))
  (flush! [_]
    (when-not (or (= stage stage-term-ack-sent)
                  (or (nil? out-pipe) (ypipe/flush! out-pipe)))
      (mbox/send! peer-mbox (cmd/new-cmd ::cmd/acitvate-read peer-pipe))))
  (-process-activate-read [this]
    (when (and (not in-active?)
               (or (= stage stage-active) (= stage stage-awaiting-delim)))
      (set! in-active? true)
      (dispatch-read-activated sink this)))
  (-process-activate-write [this msg-read-count']
    (set! peer-msg-read-count msg-read-count')
    (when (and (not out-active?) (= stage stage-active))
      (set! out-active? true)
      (dispatch-write-activated sink this)))
  (-process-hiccup [this ypipe]
    (ypipe/flush! out-pipe)
    (loop []
      (when-some [msg (ypipe/read! out-pipe)]
        (do (when-not (msg/has-more? msg)
              (set! msg-write-count (dec msg-write-count)))
            (recur))))
    (set! out-pipe ypipe)
    (set! out-active? true)
    (when (= stage stage-active)
      (dispatch-hiccuped sink this)))
  (-process-pipe-term [this]
    (enc/case-eval stage
      stage-active
      (if recv-msgs-before-term?
        (set! stage stage-awaiting-delim)
        (do
          (set! stage stage-term-ack-sent)
          (set! out-pipe nil)
          (mbox/send! peer-mbox (cmd/new-cmd ::cmd/term-ack peer-pipe))))

      stage-delim-received
      (do
        (set! stage stage-term-ack-sent)
        (set! out-pipe nil)
        (mbox/send! peer-mbox (cmd/new-cmd ::cmd/term-ack peer-pipe)))

      stage-term-req-sent
      (do
        (set! stage stage-term-req-sent-peer)
        (set! out-pipe nil)
        (mbox/send! peer-mbox (cmd/new-cmd ::cmd/term-ack peer-pipe)))
      nil))
  (-process-pipe-term-ack! [this]
    (dispatch-pipe-terminated sink this)
    (when (= stage stage-term-req-sent)
      (set! out-pipe nil)
      (mbox/send! peer-mbox (cmd/new-cmd ::cmd/term-ack peer-pipe)))
    (when-not conflate? ;; empty inbound pipe
      (while (some? (ypipe/read! in-pipe))))
    (set! in-pipe nil))
  (-process-delimiter! [this]
    (if (= stage stage-active)
      (set! stage stage-delim-received)
      (do
        (mbox/send! peer-mbox (cmd/new-cmd ::cmd/term-ack peer-pipe)) (set! out-pipe nil)
        (set! stage stage-term-ack-sent))))
  (readable? [_]
    (and in-active?
         (or (= stage-active stage) (= stage-awaiting-delim stage))
         (ypipe/readable? in-pipe)
         (msg/delimiter? (ypipe/pipe-peek in-pipe))))
  (read! [this]
    (when (and in-active?
               (or (= stage-active stage) (= stage-awaiting-delim stage))
               (ypipe/readable? in-pipe))
      (loop []
        (let [msg (ypipe/read! in-pipe)]
          (cond
            (nil? msg)
            (do (set! in-active? false) nil)

            (msg/credential? msg)
            (do (set! credential (create-blob msg))
                (recur))

            (msg/delimiter? msg)
            (do (-process-delimiter! this) nil)
            :else
            (do
              (when (not (or (msg/has-more? msg) (msg/identity? msg)))
                (set! msg-read-count (inc msg-read-count)))
              (when (and (pos? in-lower-limit)
                         (zero? (rem msg-read-count in-lower-limit)))
                (mbox/send! peer-mbox
                            (cmd/new-cmd ::cmd/activate-write peer-pipe msg-read-count)))
              msg))))))
  (-within-out-limit? [_]
    (<= (- msg-write-count peer-msg-read-count) out-limit))
  (writable? [this]
    (and out-active?
         (= stage stage-active)
         (-within-out-limit? this)))
  (write! [this msg]
    (if (writable? this)
      (let [more? (msg/has-more? msg)]
        (ypipe/write! out-pipe msg more?)
        (when-not (or more? (msg/identity? msg))
          (set! msg-write-count (inc msg-write-count)))
        true)
      false))
  (hiccup! [_]
    (when (= stage stage-active)
      (set! in-pipe (ypipe/new-ypipe msg-pipe-granularity conflate?))
      (set! in-active? true)
      (mbox/send! peer-mbox (cmd/new-cmd ::cmd/hiccup peer-pipe in-pipe))))
  (set-disconnect-msg! [this]
    (when-not (or (nil? disconnect-msg) (nil? out-pipe))
      (rollback! this)
      (ypipe/write! out-pipe disconnect-msg false)
      (flush! this)
      (set! disconnect-msg nil)))
  (send-hiccup-msg! [this hiccup-msg]
    (when-not (or (nil? hiccup-msg) (nil? out-pipe))
      (rollback! this)
      (ypipe/write! out-pipe hiccup-msg false)
      (flush! this)))
  (terminate! [this delay?]
    (set! recv-msgs-before-term? delay?)
    (enc/case-eval stage
      (list stage-term-req-sent
            stage-term-req-sent-peer
            stage-term-ack-sent) nil
      (do
        (enc/case-eval stage
          stage-active (do (mbox/send! peer-mbox (cmd/new-cmd ::cmd/term peer-pipe))
                           (set! stage stage-term-req-sent))
          stage-awaiting-delim (when-not recv-msgs-before-term?
                                 (set! out-pipe nil)
                                 (mbox/send! peer-mbox (cmd/new-cmd ::cmd/term-ack peer-pipe))
                                 (set! stage stage-term-ack-sent))
          stage-delim-received (do (mbox/send! peer-mbox (cmd/new-cmd ::cmd/term peer-pipe))
                                   (set! stage stage-term-req-sent)))
        (set! out-active? false)
        (when (some? out-pipe)
          (rollback! this)
          (ypipe/write! out-pipe (msg/new-delimiter-msg) false)
          (flush! this)))))

  Commandable)

(defn- new-pipe [in-pipe in-limit out-pipe out-limit conflate?]
  (->Pipe in-pipe
          true ;; input active
          (hwm->lwm in-limit)
          out-pipe
          true ;; output active
          out-limit
          0 ;; msg read count
          0 ;; msg write count
          0 ;; peer msg read count
          nil ;; peer pipe
          nil ;; peer mailbox
          nil ;; sink
          stage-active
          true ;; delay termination
          nil
          nil
          nil
          conflate?
          nil))

(defn new-pipe-pair [mbox1 mbox2 hwm1 hwm2 conflate1? conflate2?]
  (let [ypipe1 (ypipe/new-ypipe msg-pipe-granularity conflate1?)
        ypipe2 (ypipe/new-ypipe msg-pipe-granularity conflate2?)
        pipe1 (new-pipe ypipe1 ypipe2 hwm2 hwm1 conflate1?)
        pipe2 (new-pipe ypipe2 ypipe1 hwm1 hwm2 conflate2?)]
    (set-peer! pipe1 pipe2 mbox2)
    (set-peer! pipe2 pipe1 mbox1)
    (doto (make-array Pipe 2)
      (aset 0 pipe1)
      (aset 1 pipe2))))

(comment
  (require '[taoensso.tufte :as tufte])
  (tufte/add-basic-println-handler! {})

  (defrecord R [a b c d e f g])

  (tufte/profile
   {}
   (dotimes [_ 500]
     (tufte/p (map->R {:a 1 :b 2 :c 3 :d 4 :e 5 :f 6 :g 7})))) ;; ~50ns

  (tufte/profile
   {}
   (dotimes [_ 500]
     (tufte/p (->R 1 2 3 4 5 6 7)))) ;; ~48 ns

  ;; Conclusion: instantiating records with maps is not significantly slower, only a few ns

  (def r (->R 1 2 3 4 5 6 7))
  (def m {:a 1 :b 2 :c 3 :d 4 :e 5 :f 6 :g 7})
  (tufte/profile
   {}
   (dotimes [_ 500]
     (tufte/p (:d r)))) ;; ~48 ns
  (tufte/profile
   {}
   (dotimes [_ 500]
     (tufte/p (:d m)))) ;; ~ 50 ns

  ;; Record access not significantly faster?
  ;;
  )
