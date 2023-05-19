(import ./tk)
(import sqlite3 :as sql)
(import json)

(def db-path "./gh2.db")
(def token tk/gh-token)

(def repos  [
        {:owner "ufs-community" :name "ufs-weather-model" :token token }
        {:owner "ufs-community" :name "ufs-srweather-app" :token token }
        {:owner "ufs-community" :name "ufs-mrweather-app" :token token }
        {:owner "ufs-community" :name "regional_workflow" :token token }
])


# map of metric name to github api path

(def metrics {:views "/traffic/views"
              :clones "/traffic/clones"
              :frequency "/stats/code_frequency"
              :commits "/commits?per_page=100&page=1"
              :forks "/forks?per_page=100&page=1"
             })


(defn get-table-name [repo metric]
  (let [owner (repo :owner)
        repo-name (repo :name)]

    (string owner "/" repo-name "/" metric)))


(defn exec-query [db-path query]
  (let [db (sql/open db-path)
        rows (sql/eval db query)]
    (sql/close db)
    rows))
    

(defn get-latest [table-name]
  (let [tbl (string `"` table-name `"`)
        query (string "select timestamp from " tbl " order by timestamp desc limit 1;")
        rows (exec-query db-path query)]

    (if (>= (length rows) 0) (rows 0) nil))) 


(defn create-repo-table [db-path]
  (let [q `create table if not exists repos (
          owner text not null,
          name text not null,
          metric text not null,
          minDate text not null);`]

    (exec-query db-path q)))


(defn create-metric-table [db-path table-name]
  (let [q (string `create table if not exists ` 
                 table-name 
                 `(timestamp text not null,
                   count integer not null,
                   uniques integer not null);`)]
    
    (exec-query db-path q)))


(defn create-freq-table [db-path table-name]
  (let [q (string `create table if not exists ` 
                 table-name 
                 `(timestamp text not null,
                   additions integer not null,
                   deletions integer not null);`)]
    
    (exec-query db-path q)))


(defn create-commit-table [db-path table-name]
  (let [q (string `create table if not exists ` 
                 table-name 
                 `(timestamp text not null,
                   commits integer not null);`)]
    
    (exec-query db-path q)))


(defn create-fork-table [db-path table-name]
  (let [q (string `create table if not exists ` 
                 table-name 
                 `(fork_count integer not null);`)]
    
    (exec-query db-path q)))


(defn prune-list [lst latest]
  (if (not latest)
    lst
    (filter (fn [st] (> (st :timestamp) latest))  lst)))


(defn insert-metrics [db-path table-name lst]
  (if (not lst)
    nil
    (do
      (print "inserting metrics into " table-name)

      (def insert-query (string  "insert into " table-name " values (:timestamp, :count, :uniques);"))

      (def db (sql/open db-path))

      (each dct lst
        (print "inserting " (json/encode dct))
        (sql/eval db insert-query dct))

      (sql/close db))))


(defn insert-commits [db-path table-name lst]
  (if (not lst)
    nil
    (do
      (print "inserting metrics into " table-name)

      (def insert-query (string  "insert into " table-name " values (:timestamp, :commits);"))

      (def db (sql/open db-path))

      (each dct lst
        (print "inserting " (json/encode dct))
        (sql/eval db insert-query dct))

      (sql/close db))))


(defn insert-frequency [db-path table-name lst]
  (if (not lst)
    nil
    (do
      (print "inserting metrics into " table-name)

      (def insert-query (string  "insert into " table-name " values (:timestamp, :additions, :deletions);"))

      (def db (sql/open db-path))

      (each dct lst
        (print "inserting " (json/encode dct))
        (sql/eval db insert-query dct))

      (sql/close db))))


```
def insert_or_update_forks(con, table_name, fork_count):
    insert_sql = f'''insert or ignore into "{table_name}" (fork_count) values (?);''' 
    update_sql = f'''update "{table_name}" set fork_count = ?;''' 
    sql = insert_sql

    cursor = con.cursor()
    cursor.execute(f'select count(*) from "{table_name}";')
    exists = cursor.fetchone()[0]
    if exists:
        sql = update_sql

    cursor.execute(sql, (fork_count,))
    cursor.close()
```
    


            
(defn main [&]
  (def tbl (get-table-name (repos 0) "views"))
  (def latest "2023-01-01")
  (def lst [
             {:timestamp "2022-05-05"}
             {:timestamp "2023-01-02"}
             {:timestamp "2023-01-03"}
            ])

  (def lst2 [
              {:timestamp "2022-05-05" :count 5 :uniques 3}
              {:timestamp "2023-01-02" :count 7 :uniques 4}
              {:timestamp "2023-02-06" :count 12 :uniques 5}
            ])

  (def lst3 [
              {:timestamp "2022-05-05" :commits 5}
              {:timestamp "2023-01-02" :commits 7}
              {:timestamp "2023-02-06" :commits 10}
            ])

  (def lst4 [
              {:timestamp "2022-05-05" :additions 5 :deletions 10}
              {:timestamp "2023-01-07" :additions 20 :deletions 5}
              {:timestamp "2023-03-09" :additions 23 :deletions 30}
            ])

  #(pp (get-latest tbl))
  #(pp (prune-list lst latest))
  #(pp (get-table-name (repos 0) "views"))
  (pp (create-repo-table db-path))
  (pp (create-metric-table db-path "views"))
  (pp (create-freq-table db-path "frequency"))
  (pp (create-commit-table db-path "commits"))
  (pp (create-fork-table db-path "forks"))

  (insert-metrics db-path "views" lst2)
  (insert-commits db-path "commits" lst3)
  (insert-frequency db-path "frequency" lst4)
  
  )



