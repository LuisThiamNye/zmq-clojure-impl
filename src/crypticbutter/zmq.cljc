(ns crypticbutter.zmq
  (:require
   [farolero.core :as f :refer [signal error warn]]
   [lambdaisland.uri :as uri]
   [crypticbutter.zmq.impl.socket.base :as sock]
   [crypticbutter.zmq.impl.tcp :as tcp]
   [crypticbutter.zmq.impl.msg :as msg]
   [crypticbutter.zmq.impl.socket.req :as req]
   [crypticbutter.zmq.impl.ctx :as ctx])
  (:import
   (java.lang Class Runtime)
   (crypticbutter.zmq.impl.msg Msg)))

;; Context
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defonce *context-registry (atom {}))

(defn create-context [])

(defn terminate-context! [ctx]
  (ctx/terminate! ctx))

(defmacro add-hook [ctx hook & body]
  (when-not (#{:before-termination :after-termination} hook)
    (throw (IllegalArgumentException. (str hook " is not a valid hook."))))
  `(swap! *context-registry update-in [~ctx ~hook] (fnil conj [])
          (fn [] ~@body)))

(defonce *config {:auto-shutdown-hook? true})

(.addShutdownHook (Runtime/getRuntime)
                  (Thread. (fn []
                             (when (:auto-shutdown-hook? @*config)
                               (doseq [ctx (keys @*context-registry)]
                                 (terminate-context! ctx))))))

;; Sockets
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- scheme+authority [endpoint]
  (subvec (re-matches #"(\w+)://(.+)" endpoint) 1))

(defn bind
  "Accept incoming connections on a socket.

  Endpoint must be local."
  [ctx socket endpoint]
  (let [[scheme authority] (scheme+authority endpoint)]
    (case scheme
      "tcp" (tcp/bind! ctx socket authority)
      (error ::error.unsupported-protocol
             {:message "The requested transport protocol is not supported."}))))

(defn connect
  "Create an outgoing connection from a socket"
  [ctx socket endpoint]
  (let [[scheme authority] (scheme+authority endpoint)]
    (case scheme
      "tcp" (tcp/connect! ctx socket endpoint))))

(defn disconnect [ctx socket endpoint]
  (sock/terminate-endpoint! socket endpoint))

(defn socket
  ([ctx sock-type options]
   (let [{bind-ep :bind
          connect-ep :connect} options
         sock (ctx/create-socket! ctx (case sock-type
                                       :req req/req-creator))]
     (when (some? bind-ep)
       (bind ctx socket bind-ep))
     (when (some? connect-ep)
       (connect ctx socket connect-ep))
     sock))
  ([ctx sock-type] (socket ctx sock-type {})))

(defn close!
  "Permanently closes a socket"
  [socket]
  (error "Not implemented"))

;; Messaging
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn data->frame [data flags]
  (cond
    (string? data) (msg/new-msg flags (.getBytes ^String data))
    (bytes? data) (msg/new-msg flags data)
    :else (error :error/unsupported)))

(defprotocol CompoundMessage
  (->frames [_]))

(extend-protocol CompoundMessage
  (Class/forName "[B") ;; Must be first or breaks
  (->frames [array-of-bytes]
    (doto (make-array Msg 1)
      (aset 0 (data->frame array-of-bytes 0))))

  String
  (->frames [string]
    (doto (make-array Msg 1)
      (aset 0 (data->frame string 0))))

  clojure.lang.Sequential
  (->frames [xs]
    (let [xs-count (count xs)
          arr (make-array Msg xs-count)]
      (if (instance? clojure.lang.Indexed xs)
        (loop [idx 0]
          (when (< idx xs-count)
            (aset arr idx (data->frame (nth xs idx) (if (= idx (dec xs-count))
                                                      0 msg/flag-more)))
            (recur (inc idx))))
        (loop [xs' xs
               idx 0]
          (when-some [x (first xs')]
            (aset arr idx (data->frame x 0))
            (recur (next xs) (inc idx))))))))

(defn send
  ([ctx socket msg {:keys [timeout]}] ;; TODO timeout
   (let [frames (->frames msg)
         frame-count (alength frames)]
     (loop [idx 0]
       (when (< idx frame-count)
         (sock/send! socket (aget frames idx))
         (recur (inc idx))))))
  ([ctx socket msg] (send ctx socket msg {})))

(defn recv
  ([ctx socket {:keys [timeout stringify]}] ;; TODO timeout
   (when-some [init-msg (sock/recv! socket)]
     (loop [frames [(msg/data init-msg)]]
       (if (sock/awaiting-more? socket)
         (recur (->> (sock/recv! socket) msg/data (conj frames)))
         frames))))
  ([ctx socket] (recv ctx socket {})))

(comment

  ;; => (:scheme :user :password :host :port :path :query :fragment)
  (-> (uri/uri "tcp://*:45")
      ;; (assoc :port 8888)
      :path)

  ;; transports:
  ;; tcp
  ;; ipc
  ;; inproc
  ;; pgm, epgm
  ;; vmci

  ;; Datomic error categories
  ;; unavailable
  ;; interrupted
  ;; incorrect
  ;; forbidden
  ;; unsupported
  ;; not-found
  ;; conflict (with state of system)
  ;; fault (something went wrong that we do not know)
  ;; busy
  ;;
  (require '[crypticbutter.zmq] :reload-all)
  ;;
  )
