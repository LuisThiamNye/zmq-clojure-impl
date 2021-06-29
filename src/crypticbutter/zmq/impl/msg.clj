(ns crypticbutter.zmq.impl.msg)

(def ^:const flag-more 1)
(def ^:const flag-cmd 2)
(def ^:const flag-credential 32)
(def ^:const flag-identity 64)
(def ^:const flag-shared 128)

(def ^:const type-data 0)
(def ^:const type-join 1)
(def ^:const type-leave 2)
(def ^:const type-delim 3)

(defmacro has-flag? [flags flag]
  `(= ~flag (bit-and ~flags ~flag)))

(defprotocol MsgProtocol
  (has-more? [_])
  (remove-flags [_])
  (delimiter? [_])
  (credential? [_])
  (command? [_])
  (identity? [_]))

(deftype Msg
    ;; Representation of data to be sent in a single frame.
         [buffer
          ^:int flags
          ^:int msg-type
          metadata]
  MsgProtocol
  (has-more? [_]
    (has-flag? flags flag-more))
  (remove-flags [_ unwanted-flags]
    (bit-and flags (bit-not unwanted-flags)))
  (credential? [_]
    (has-flag? flags flag-credential))
  (command? [_]
    (has-flag? flags flag-cmd))
  (identity? [_]
    (has-flag? flags flag-identity))
  (delimiter? [_]
    (= msg-type type-delim))
  (join? [_]
    (= msg-type type-join))
  (leave? [_]
    (= msg-type type-leave))
  (data? [_]
    (= msg-type type-data)))

(defn put-uint32! [buffer value]
  (doto buffer
    (.put (byte (bit-and 256 (bit-shift-right value 24))))
    (.put (byte (bit-and 256 (bit-shift-right value 16))))
    (.put (byte (bit-and 256 (bit-shift-right value 8))))
    (.put (byte (bit-and 256 value)))))

(defn- new-buffer [buf-size]
  (doto (java.nio.ByteBuffer/wrap (byte-array buf-size))
    (.order java.nio.ByteOrder/BIG_ENDIAN)))

(defn new-msg
  ([flags]
   (new-msg flags nil 0))
  ([flags buf-val buf-size]
   (let [buf (new-buffer buf-size)]
     (->Msg (case buf-size
              4 (put-uint32! buf buf-val)
              0 buf)
            flags
            nil ;; metadata
            type-data))))

(defn new-delimiter-msg []
  (->Msg (new-buffer 0)
         0 ;; flags
         type-delim
         nil ;; Metadata
         ))


(comment
  (.array (put-uint32! (doto (java.nio.ByteBuffer/wrap (byte-array 4))
                         (.order java.nio.ByteOrder/BIG_ENDIAN))
                       3))

  (let [b (byte-array 8)]
    (.putInt (java.nio.ByteBuffer/wrap b) 3)
    (java.util.Arrays/copyOfRange b 4 8))

  (defprotocol P
    (f [_ ^long x]))

  (deftype R []
    P
    (f [_ x] Long/MAX_VALUE))

  (.f (R.) "")

  (definterface I
    (g [^int x]))
  (definterface I2
    (g1 [^int x]))

  (defrecord Q []
    I
    (g [_ x] 4)
    I2
    (g1 [_ x] 7)
    P
    (f [_ x] 8))

  (.f (Q.) 4)

;;
  )
