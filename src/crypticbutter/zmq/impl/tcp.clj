(ns crypticbutter.zmq.impl.tcp
  (:require
   [clojure.string :as str]
   [farolero.core :as f :refer [signal error warn]]
   [crypticbutter.snoop :refer [>defn => >defn-]]
   [crypticbutter.zmq.impl.ctx :as ctx]
   [crypticbutter.zmq.impl.socket.base :as base])
  (:import
   (java.nio.channels ServerSocketChannel)
   (java.net InetAddress InetSocketAddress StandardSocketOptions)
   (crypticbutter.zmq.impl.ctx Ctx)))

(def windows? (str/starts-with? (System/getProperty "os.name")
                                "Windows"))

(def tcp-min-dyn-port 49152)
(def tcp-max-dyn-port 65535)

(defn bind-to-random-port!
  ([socket endpoint]
   (bind-to-random-port!
    socket endpoint tcp-min-dyn-port tcp-max-dyn-port))
  ([socket endpoint min-port max-port]
   (loop [port min-port]
     (if (<= port max-port)
       (if (try (.bind socket (str endpoint ":" port))
                (catch Exception _ false))
         true
         (recur (inc port)))
       false))))

(defn parse-tcp-auth [authority ipv6?]
  (let [[_ input-host input-port] (re-matches #"(.+?)(?::(\d+))" authority)]
    [(if (identical? "*" input-host)
       (if ipv6?
         "::"
         "0.0.0.0")
       input-host)
     (if (or (nil? input-port)
             (identical? "*" input-port))
       0
       (Integer/parseInt input-port))]))

(defn- socket-address
  [host port ipv6?]
  [string? nat-int? boolean? => :any]
  (try
    (let [ip-addrs (.getAllByName InetAddress host)
          chosen-ip-addr (if ipv6?
                           (or (some #(instance? java.net.Inet6Address %) ip-addrs)
                               (first ip-addrs))
                           (some #(instance? java.net.Inet4Address %) ip-addrs))]
      (if (some? chosen-ip-addr)
        (InetSocketAddress. chosen-ip-addr port)
        (error ::error.address-not-local
               {:message (str host " not available for ipv6 enable state: " ipv6?)})))
    (catch java.net.UnknownHostException e
      (error ::error.address-not-local
             {:message (.getMessage e)}))))

(deftype TcpListener
  [address
   sock-chan]

  )


(defprotocol TcpCtx
  (bind! [_ socket authority])
  (connect! [_ socket authority]))

(extend-type Ctx
  TcpCtx
  (bind! [ctx socket-base authority]
    (let [sock-opts (base/get-options socket-base)
          sock-chan (.open ServerSocketChannel)]
      (if-some [io-thread (ctx/choose-io-thread ctx (:affinity sock-opts))]
        (try
          (let [ipv6? (:ipv6-enabled sock-opts)
                [host port] (parse-tcp-auth authority ipv6?)
                snd-buf (:send-buffer-size sock-opts)
                rcv-buf (:recv-buffer-size sock-opts)
                sock-chan (doto sock-chan
                            (.configureBlocking  false)
                            (cond-> (some? snd-buf)
                              (.setOption (.-SO_SNDBUF StandardSocketOptions)
                                          snd-buf))
                            (cond-> (some? rcv-buf)
                              (.setOption (.-SO_SNDBUF StandardSocketOptions)
                                          rcv-buf))
                            (cond-> windows?
                              (.setOption (.-SO_REUSEADDR StandardSocketOptions)
                                          true)))
                sock-obj (.socket sock-chan)]
            (.bind sock-obj
                   (socket-address host port ipv6?)
                   (:max-backlog sock-opts))
            (let [uri-str (str "tcp://" host \: (.getPort sock-obj))]
              (base/add-endpoint! socket-base uri-str (->TcpListener authority sock-chan) nil)))
          (catch java.io.IOException _
            (try (.close sock-chan)
                 (catch java.io.IOException _
                   ;; TODO
                   ))
            (error ::error.address-in-use
                   {:message "The requested address is already in use."})))
        (error ::error.no-io-thread
               {:message "No I/O thread is available to accomplish the task."}))))
  (connect! [ctx socket-base authority]))
