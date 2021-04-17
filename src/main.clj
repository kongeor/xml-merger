(ns main
  (:require [next.jdbc :as jdbc]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [next.jdbc.result-set :as rs]
            [merger]))

(defn xml-file? [f]
  (.endsWith (str f) ".xml"))

(defn parse-file [f]
  "Extracts the region and the patent id from the provided file. Expects a string."
  (let [path-tokens (string/split f #"/")
        file-name (last path-tokens)
        parts (string/split file-name #"-")]
    {:region (first parts) :patent-id (second parts)}))

;; not as optimal but should be all right
(defn valid-file? [f]
  (let [file-name (last (string/split (str f) #"/"))]
    (string/includes? file-name "-")))

#_(def _example-file (str (first (filter xml-file? (file-seq (io/file "resources/dataset/CN20140101"))))))

#_(parse-file _example-file)

(comment
  ;; 73868
  (count (filter xml-file? (file-seq (io/file "resources/dataset")))))

;; db stuff

(def db {:dbtype "h2" :dbname "resources/db/patents"})

(def ds (jdbc/get-datasource db))

(def ds-opts (jdbc/with-options ds {:builder-fn rs/as-unqualified-lower-maps}))

(defn create-tables [ds]
  (jdbc/execute! ds ["
  create table if not exists files (
    id bigint auto_increment primary key,
    region varchar(2),
    patentid varchar(20),
    file varchar(4000)
  );

  create index if not exists files_region_patentid_idx on files(region, patentid);

  create table if not exists patents (
    id biging auto_increment primary key,
    region varchar(2),
    patentid varchar(20),
    count
  )

  "]))

(comment
  (create-tables ds-opts))

(defn insert-file [ds file]
  (let [file (str file)
        {:keys [region patent-id]} (parse-file file)]
    (jdbc/execute-one! ds
      (concat
        ["insert into files(region, patentid, file) values(?, ?, ?)"]
        [region patent-id file]))))

(defn files->param-tuple [f]
  (let [f' (str f)
        {:keys [region patent-id]} (parse-file f')]
    [region patent-id f']))

(defn insert-files [ds files]
  (let [n (count files)
        placeholders (string/join ", " (repeat n "(?, ?, ?)"))
        params (mapcat files->param-tuple files)
        q (cons (str "insert into files(region, patentid, file) values" placeholders)
                      params)]
    (jdbc/execute! ds q)))


(defn q [ds]
  (jdbc/execute! ds-opts ["select region, patentid, count(*) as cnt from files group by region, patentid having cnt > 1"]))

(defn q2 [ds]
  (jdbc/execute! ds-opts ["select * from files where patentid = '2681204'"]))

(comment
  (let [entries (q2 ds-opts)
        patent-id (-> entries first :patentid)
        files (map :file entries)]
    (merger/merge-and-write "resources/output" patent-id files)))

(comment
  (clojure.pprint/pprint (q2 ds-opts)))

(def batch-size 200)

;; processing

(defn process-dataset []
  (let [all-files (filter valid-file? (filter xml-file? (file-seq (io/file "resources/dataset"))))
        batches (partition-all 200 all-files)]
    (doall
      (map-indexed
        (fn [idx batch]
          (clojure.tools.logging/info "Processing batch" idx)
          (insert-files ds-opts batch)) batches))))

(defn process-dataset []
  )

(comment
  (process-dataset))

