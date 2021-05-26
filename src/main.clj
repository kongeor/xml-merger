(ns main
  (:require [next.jdbc :as jdbc]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [next.jdbc.result-set :as rs]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [merger])
  (:gen-class))

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

#_(def db {:dbtype "h2" :dbname "resources/db/patents"})

#_(def ds (jdbc/get-datasource db))

#_(def ds-opts (jdbc/with-options ds {:builder-fn rs/as-unqualified-lower-maps}))

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
    id bigint auto_increment primary key,
    region varchar(2),
    patentid varchar(20),
    count int
  );

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
  (jdbc/execute! ds ["select region, patentid, count(*) as cnt from files group by region, patentid having cnt > 1"]))

(defn patent-files-q [ds patentid]
  (jdbc/execute! ds ["select * from files where patentid = ?" patentid]))

(comment
  (q2 ds-opts))

(comment
  (let [entries (patent-files-q ds-opts "2681204")
        patent-id (-> entries first :patentid)
        files (map :file entries)]
    (merger/merge-and-write "resources/output" patent-id files)))

(comment
  (clojure.pprint/pprint (q ds-opts)))

(def batch-size 200)

;; processing

(defn process-dataset-files [ds patent-dir]
  (let [all-files (filter valid-file? (filter xml-file? (file-seq (io/file patent-dir))))
        batches (partition-all 200 all-files)]
    (doall
      (map-indexed
        (fn [idx batch]
          (clojure.tools.logging/info "Processing batch" idx)
          (insert-files ds batch)) batches))))

(comment
  (process-dataset-files))

(comment
  (count (jdbc/execute! ds-opts ["select region, patentid, count(*) as cnt from files group by region, patentid"])))

(comment
  (jdbc/execute! ds-opts ["select * from patents"]))


(defn q3 [ds]
  (jdbc/execute! ds ["select region, patentid, count(*) as cnt from files group by region, patentid having"]))

(defn process-patents [ds]
  (reduce
    (fn [_ row]
      (jdbc/execute-one! ds
        (concat
          ["insert into patents(region, patentid, count) values(?, ?, ?)" (:region row) (:patentid row) (:cnt row)])))
    nil
    (jdbc/plan ds ["select region, patentid, count(*) as cnt from files group by region, patentid"])))

(comment
  (process-patents))

#_(def file-count-size 1000)

#_(def output-dir "resources/output")

(defn process-region-batch [ds output-dir folder-file-count region batch-idx]
  (let [limit folder-file-count
        offset (* batch-idx folder-file-count)
        folder (str output-dir "/" region "/" batch-idx)]
    (log/info "Processing" region "batch id" batch-idx)
    (.mkdirs (java.io.File. folder))
    (reduce
      (fn [_ row]
        (let [files (patent-files-q ds (:patentid row))]
          (merger/merge-and-write folder (:patentid row) (map :file files))))
      nil
      (jdbc/plan ds ["select region, patentid, from patents where region = ? limit ? offset ?" region limit offset]))))

(comment
  (.mkdirs (java.io.File. "resources/output/yo1/man")))

(comment
  (process-region-batch "WO" 0))

(defn regions-counts-q [ds]
  (jdbc/execute! ds ["select region, count(*) as cnt from patents group by region"]))

(defn process-region [ds output-dir n {:keys [region cnt]}]
  (let [batch-ids (range 0 (inc (int (/ cnt n))))]
    (doall (pmap #(process-region-batch ds output-dir n region %) batch-ids))))

#_(regions-counts-q ds-opts)

#_(range (inc (int (/ 2300 1000))))

(defn process-regions [ds output-dir n]
  (let [regions (regions-counts-q ds)]
    (doall (map #(process-region ds output-dir n %) regions))))

(comment
  (process-regions))

(defn check-existing-folders! [db-dir output-dir force]
  (let [db-dir (java.io.File. db-dir)
        output-dir (java.io.File. output-dir)]
    (when
      (or
        (.exists db-dir)
        (.exists output-dir))
      (log/info "Database and/or output directories exist")
      (if force
        (do
          (log/warn "Removing existing directories")
          (.delete db-dir)
          (.delete output-dir))
        (do
          (System/exit 1))))))

(defn start! [opts]
  (let [db-dir (:db-dir opts)
        output-dir (:output-dir opts)
        force (:force opts)]
    (let [ds (jdbc/get-datasource {:dbtype "h2" :dbname (str db-dir "/patents")})
          ds-opts (jdbc/with-options ds {:builder-fn rs/as-unqualified-lower-maps})]
      (check-existing-folders! db-dir output-dir force)
      (create-tables ds-opts)
      (process-dataset-files ds-opts (:patent-dir opts))
      (process-patents ds)
      (process-regions ds-opts output-dir (:dir-file-count opts)))))

;; cli

(defn usage [options-summary]
  (->> ["A program to merge xml patent files, organize them per region and to break them down into folders."
        ""
        "Usage: java -jar xml-merger.jar -n 2000 --db-dir temp/db -o temp/output -p temp/dataset"
        ""
        "Options:"
        options-summary]
    (string/join \newline)))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
    (string/join \newline errors)))

(def cli-options
  ;; An option with a required argument
  [["-d" "--db-dir DB_DIR" "Temporary database folder"
    :validate [#(not (nil? %))]]
   ;; A non-idempotent option (:default is applied first)
   ["-p" "--patent-dir PATENT_DIR" "Patent directory. Folder containing folders such as CN20140101, US20140225 etc."]
   ["-o" "--output-dir OUT_DIR" "Output directory. Folder to store the grouped patents. "]
   ["-n" "--dir-file-count DIR_FILE_COUNT" "How many files each foldr should contain."
    :parse-fn #(Integer/parseInt %)]
   ["-f" "--force" "Force the removal of existing output and database directories from potential previous runs."]
   ["-h" "--help"]])

(defn validate-args
  "Validate command line arguments. Either return a map indicating the program
  should exit (with a error message, and optional ok status), or a map
  indicating the action the program should take and the options provided."
  [args]
  (let [{:keys [options errors summary]} (cli/parse-opts args cli-options)]
    (cond
      (:help options) ; help => exit OK with usage summary
      {:exit-message (usage summary) :ok? true}
      errors ; errors => exit with description of errors
      {:exit-message (error-msg errors)}
      ;; custom validation
      (nil? (:db-dir options))
      {:exit-message "db-dir is required"}
      (nil? (:output-dir options))
      {:exit-message "output-dir is required"}
      (nil? (:patent-dir options))
      {:exit-message "patent-dir is required"}
      :else ; all good
      {:options options})))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(comment
  (validate-args ["asdf" "-n" "1" "--db-dir" "resources/db" "-o" "resources/output" "-p" "resources/dataset"]))

(defn -main [& args]
  (let [{:keys [action options exit-message ok?]} (validate-args args)]
    (if exit-message
      (exit (if ok? 0 1) exit-message)
      (start! options))))

