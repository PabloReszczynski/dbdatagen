(ns datadata.core
  (:require ["@faker-js/faker" :refer [faker]]
            [nbb.core :refer :all]
            [goog.string :refer [format]]
            [promesa.core :as p]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [applied-science.js-interop :as j]
            [clojure.pprint :refer [pprint]])
  (:require-macros [cljs.core :refer [this-as]]))

(def tty? (boolean js/process.stdin.isTTY))

(js/process.stdout.on "error"
  (fn [err]
    (when (= err.code "EPIPE")
      (js/process.exit 0))))

(def config
  (let [args (->> js/process.argv
                  (drop 3)
                  (partition 2)
                  (map (fn [[k v]] [(str/replace k "-" "") v]))
                  (into {}))
        config-file (args "c")
        config-input (args "i")
        config-pipe (not tty?)]
    (cond
      config-file (p/then (slurp config-file) edn/read-string)
      config-input (edn/read-string config-input)
      config-pipe (p/then (slurp js/process.stdin.fd) edn/read-string)
      :else false)))

(defn format-type [v]
  (cond
    (string? v) (format "'%s'" v)
    (map? v) (format "'%s'" (js/JSON.stringify (clj->js v)))
    :else v))

(defn gen-insert [table-name rows]
  (for [row rows]
    (let [col-names (->> (keys row)
                         (str/join ","))
          values (->> (vals row)
                      (map format-type)
                      (str/join ","))]
      (format "INSERT INTO \"%s\" (%s) VALUES(%s);" table-name col-names values))))

(defn find-relation [tables relation]
  (let [[table-name idx col] relation]
    (if-let [table (get tables table-name)]
      (if-let [row (get-in table [idx col])]
        row
        (throw (js/Error. (str "no column found for relation " col))))
      (throw (js/Error. (str "no table found for relation " table-name))))))

(defn create-row [tables columns]
  (into {}
    (for [column columns]
      (let [col-name (first column)
            col-type (second column)]
        [col-name
         (case (first col-type)
           :faker (j/apply-in faker (str/split (second col-type) ".")
                              (clj->js (drop 2 column)))
           :static (second col-type)
           :relation (find-relation tables (rest col-type)))]))))

(defn create-rows [tables columns n]
  (into []
    (for [_ (range n)]
      (create-row tables columns))))

(p/let [config config]
  (let [tables (:tables config)
        queries (reduce
                  (fn [tables [table-name table]]
                    (let [rows (create-rows tables
                                            (get table :columns)
                                            (get table :count 1))]
                      (assoc tables table-name rows)))
                  {}
                  tables)
        insertions (mapcat #(apply gen-insert %) queries)
        header "BEGIN;\n"
        footer "\nCOMMIT;"
        plan (str
               header
               (str/join "\n" insertions)
               footer)]

    (js/process.stdout.write plan)))




