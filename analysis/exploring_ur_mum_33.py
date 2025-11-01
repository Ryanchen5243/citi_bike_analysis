import os
from xml.dom.pulldom import default_bufsize
from dotenv import load_dotenv
import duckdb
from collections import defaultdict
import pandas as pd
import re
def main():
    load_dotenv()
    conn = duckdb.connect(':memory:')
    conn.execute('INSTALL httpfs;')
    conn.execute('LOAD httpfs;')
    # authenication
    conn.execute(f"SET s3_region='us-east-1';")
    conn.execute(f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}';")
    conn.execute(f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}';")
    
    columns_mapping = defaultdict(list)
    nyc_year_range = range(2013,2026)
    jc_year_range = range(2015,2026)
    dtypes_hash = defaultdict(list) # col_name -> type+
    # for year in jc_year_range:
    #     print(f"Processing year: {year}")
    #     s3_source = f"read_parquet('s3://citibike-nycdata/parquet_files/jc_files/{year}/*')"
    #     conn.execute(f"""
    #             CREATE OR REPLACE VIEW nyc_view AS SELECT * FROM {s3_source}
    #                  """)
    #     result = conn.execute("SELECT * FROM nyc_view;")
    #     sample = result.fetchone()
    #     for i in range(len(conn.description)):
    #         col_name,col_type = str(conn.description[i][0]),str(conn.description[i][1])
    #         value = sample[i]
    #         print(f"{col_name},{col_type},{value}",flush=True)
    #         dtypes_hash[(col_name,col_type)].append(year)
    #     print("----------------------------------------------")
    # print("data types detected -> ")
    # for k,v in dtypes_hash.items():
    #     print("col: ",k," : \n",v)
    schema_cols = [
        "trip_duration",
        # "trip_date",
        "start_time",
        "stop_time",
        "start_station_id",
        "start_station_name",
        "start_station_lat",
        "start_station_long",
        "end_station_id",
        "end_station_name",
        "end_station_lat",
        "end_station_long",
        "user_type"
    ]
    col_mapping = {"nyc": {"2013" : {"col_names" : ["tripduration as trip_duration","starttime as start_time","stoptime as stop_time", '"start station id" as start_station_id','"start station name" as start_station_name','"start station latitude" as start_station_lat','"start station longitude" as start_station_long','"end station id" as end_station_id','"end station name" as end_station_name','"end station latitude" as end_station_lat','"end station longitude" as end_station_long','usertype as user_type'], 
                                     "filter_clause" : 'WHERE starttime IS NOT NULL AND stoptime IS NOT NULL AND "start station id" IS NOT NULL AND "start station latitude" IS NOT NULL AND "start station longitude" IS NOT NULL AND "end station id" IS NOT NULL AND "end station latitude" IS NOT NULL AND "end station longitude" IS NOT NULL'},
                           "2014" : {"col_names" : ["tripduration as trip_duration","starttime as start_time","stoptime as stop_time", '"start station id" as start_station_id','"start station name" as start_station_name','"start station latitude" as start_station_lat','"start station longitude" as start_station_long','"end station id" as end_station_id','"end station name" as end_station_name','"end station latitude" as end_station_lat','"end station longitude" as end_station_long','usertype as user_type'],
                                    "filter_clause" : ""},
                           "2015" : {"col_names" : ["tripduration as trip_duration","starttime as start_time","stoptime as stop_time", '"start station id" as start_station_id','"start station name" as start_station_name','"start station latitude" as start_station_lat','"start station longitude" as start_station_long','"end station id" as end_station_id','"end station name" as end_station_name','"end station latitude" as end_station_lat','"end station longitude" as end_station_long','usertype as user_type'], 
                                     "filter_clause" : ""},
                           "2016" : {"col_names" : ["tripduration as trip_duration","starttime as start_time","stoptime as stop_time", '"start station id" as start_station_id','"start station name" as start_station_name','"start station latitude" as start_station_lat','"start station longitude" as start_station_long','"end station id" as end_station_id','"end station name" as end_station_name','"end station latitude" as end_station_lat','"end station longitude" as end_station_long','usertype as user_type'], 
                                     "filter_clause" : ""},
                           "2017" : {"col_names" : ["tripduration as trip_duration","starttime as start_time","stoptime as stop_time", '"start station id" as start_station_id','"start station name" as start_station_name','"start station latitude" as start_station_lat','"start station longitude" as start_station_long','"end station id" as end_station_id','"end station name" as end_station_name','"end station latitude" as end_station_lat','"end station longitude" as end_station_long','usertype as user_type'], 
                                    "filter_clause" : 'WHERE "start station latitude" IS NOT NULL AND "end station latitude" IS NOT NULL AND "end station longitude" IS NOT NULL'},
                           "2018" : {"col_names" : ["tripduration as trip_duration","starttime as start_time","stoptime as stop_time", '"start station id" as start_station_id','"start station name" as start_station_name','"start station latitude" as start_station_lat','"start station longitude" as start_station_long','"end station id" as end_station_id','"end station name" as end_station_name','"end station latitude" as end_station_lat','"end station longitude" as end_station_long','usertype as user_type'],
                                    "filter_clause" : 'WHERE "start station id" IS NOT NULL AND "end station id" IS NOT NULL'},
                           "2019" : {"col_names" : ["tripduration as trip_duration","starttime as start_time","stoptime as stop_time", '"start station id" as start_station_id','"start station name" as start_station_name','"start station latitude" as start_station_lat','"start station longitude" as start_station_long','"end station id" as end_station_id','"end station name" as end_station_name','"end station latitude" as end_station_lat','"end station longitude" as end_station_long','usertype as user_type'], 
                                    "filter_clause" : 'WHERE "start station id" IS NOT NULL AND "end station id" IS NOT NULL'},
                           "2020" : {"col_names" : [], "filter_clause" : ""},
                           "2021" : {"col_names" : "", "filter_clause" : ""},
                           "2022" : {"col_names" : "", "filter_clause" : ""},
                           "2023" : {"col_names" : "", "filter_clause" : ""},
                           "2024" : {"col_names" : "", "filter_clause" : ""},
                           "2025" : {"col_names" : "", "filter_clause" : ""}
                           }, 
                    "jc": {"2015" : {"col_names" : "", "filter_clause" : ""},"2016" : {"col_names" : "", "filter_clause" : ""},"2017" : {"col_names" : "", "filter_clause" : ""},"2018" : {"col_names" : "", "filter_clause" : ""},"2019" : {"col_names" : "", "filter_clause" : ""},"2020" : {"col_names" : "", "filter_clause" : ""},"2021" : {"col_names" : "", "filter_clause" : ""},"2022" : {"col_names" : "", "filter_clause" : ""},"2023" : {"col_names" : "", "filter_clause" : ""},"2024" : {"col_names" : "", "filter_clause" : ""},"2025" : {"col_names" : "", "filter_clause" : ""}}}
    # for n in jc_year_range:
    #     print(f"\"{n}\" : {{\"col_names\" : \"\", \"filter_clause\" : \"\"}},",end="")
    # print(col_mapping)
    for city in ["nyc","jc"]:
        year_range = nyc_year_range if city == "nyc" else jc_year_range
        for year in [2020]:
            conn.execute(f"""
                    CREATE OR REPLACE VIEW raw_view AS SELECT * FROM {f"read_parquet('s3://citibike-nycdata/parquet_files/{city}_files/{year}/*')"}
                        """)
            # ['column_name', 'column_type', 'min', 'max', 'approx_unique', 'avg', 'std', 'q25', 'q50', 'q75', 'count', 'null_percentage']
            result = conn.execute("SELECT COUNT(*) FROM raw_view").fetchall()
            raw_view_total = result[0][0]
            print(f"Total entries found for {year} -> {raw_view_total}")
            print("raw_view columns -> \n",[(col[0],col[1]) for col in conn.execute("DESCRIBE raw_view").fetchall()])
            select_cols = ','.join(col_mapping[city][str(year)]["col_names"])
            result = conn.execute(f'CREATE OR REPLACE VIEW temp_view AS SELECT {select_cols} FROM raw_view {col_mapping[city][str(year)]["filter_clause"]}')
            temp_view_cols = [(col[0],col[1]) for col in conn.execute("DESCRIBE temp_view").fetchall()]
            print("temp_view columns -> \n",temp_view_cols)
            # check against schema
            print("checking schema validity")
            assert len(temp_view_cols) == len(schema_cols), "Incorrect column count in temp view!"
            for col_name in schema_cols:
                assert col_name in [c for c,_ in temp_view_cols], f"col {col_name} missing in temp view"
            print("done checking schema validity :))")
            # end check against schema
            # print("sample from temp_view ->")
            # print(conn.execute("select * from temp_view limit 5").fetchall())
            print([(e[0],e[-1]) for e in conn.execute("SUMMARIZE(SELECT * FROM temp_view);").fetchall()])
            query = []
            for c in schema_cols:
                query.append(f"COUNT(*) - COUNT({c})")
            result = conn.execute(f"""
                            SELECT {','.join(query)} FROM temp_view
                               """).fetchall()
            for i,col in enumerate(schema_cols):
                print(f"Null Count -- {col} -> ",result[0][i])
                assert result[0][i] == 0, "nil value unhandled!!"
            temp_view_total = conn.execute("SELECT COUNT(*) FROM temp_view").fetchall()[0][0]
            print(f"reduced {raw_view_total} by {raw_view_total-temp_view_total} -> {temp_view_total}")
            print("sample query -> ")
            print(conn.execute("SELECT * FROM temp_view LIMIT 5").fetchall())
            print("---------------------------------------------------")

            # filter to new view entries where any of the following col c1,c2,c3,c4 are null

            # print([c[0] for c in conn.description])
            # print(f"{'Column':25} | {'Type':10} | {'Null %':>7} | {'Count':>8} | {'Avg':>10} | {'Std':>10}")
            # print("-" * 80)
            # for e in result:
            #     col_name, col_type, _, _, _, avg, std, _, _, _, count, null_pct = e
            #     print(f"{col_name:25} | {col_type:10} | {float(null_pct):7.2f} | {count:8} | {avg or '':>10} | {std or '':>10}")

if __name__ == '__main__':
    main()