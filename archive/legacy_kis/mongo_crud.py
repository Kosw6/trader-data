from pymongo import MongoClient
import os
from dotenv import load_dotenv, find_dotenv
import time
from datetime import datetime

dotenv_file = find_dotenv()
load_dotenv(dotenv_file, override=True)
mongo_username = os.getenv("MONGO_USERNAME")
mongo_password = os.getenv("MONGO_PASSWORD")

def connect_mongo():
    try :
        client = MongoClient(
        f"mongodb://{mongo_username}:{mongo_password}@localhost:27017/trader?authSource=admin"
        )
        print("connect DB")
        return client
    except Exception as e:
        print("connect_mongo failed:", e)


def save_stock_list(stock_list) :
    try :
        client = connect_mongo()
        collection = client["trader"]["stock_list"] 
        result = collection.insert_many(stock_list)
        # print(result.inserted_ids)
    except Exception as e:
        print("save_stock_list : "+e)
    finally :
        client.close()

def save_stock_ts_data(stock_ts_data_list) :
    try :
        client = connect_mongo()
        collection = client["trader"]["stock_ts"] 
        result = collection.insert_many(stock_ts_data_list)
    except Exception as e:
        print("save_stock_list : "+e)
    finally :
        client.close()

def find_stock_list():
    try :
        client = connect_mongo()
        collection = client["trader"]["stock_list"] 
        return list(collection.find({}, {'_id': 0,'excd':1,'symb': 1}))
    except Exception as e:
        print("save_stock_list : "+e)
    finally :
        client.close()

def find_stock_data_ts_list():
    try :
        client = connect_mongo()
        collection = client["trader"]["stock_ts"]
        pipeline = [
            {"$group": {"_id": "$symb"}},  # symb 필드 기준으로 그룹화 (중복 제거)
            {"$project": {"_id": 0, "symb": "$_id"}}  # _id 제거하고 symb 필드 반환
        ]
        result = collection.aggregate(pipeline)
        unique_symb = [doc["symb"] for doc in result]
        return unique_symb
    except Exception as e:
        print("find_stock_data_list : "+e)
    finally :
        client.close()

def find_stock_ts_recent(symb):
    try :
        client = connect_mongo()
        collection = client["trader"]["stock_ts"]
        result = collection.find({"symb": symb}).sort('timestamp',-1).limit(1)
        return result.next()
    except Exception as e:
        print("find_stock_data_list : "+e)
    finally :
        client.close()
#몽고디비 시계열구조와 기존 구조의 조회성능 비교
def cmp_timeseries():
    try :
        client = connect_mongo()
        collection1 = client["trader"]["stocks"]
        collection2 = client["trader"]["stock_ts"]
        start_time = time.perf_counter_ns()
        # print(start_time)
        result1 = collection1.find_one({"symb":"APPL"})
        end_time = time.perf_counter_ns()
        # print(end_time)
        time_diff = end_time-start_time
        print("기존데이터 : ",time_diff)
        # print(list(result1))
        start_time = time.perf_counter_ns()
        result2 = collection2.find_one({"symb":"APPL"})
        end_time = time.perf_counter_ns()
        time_diff = end_time-start_time
        print("수정데이터 : ",time_diff)
        #find에서는 10배차이, find_one에서는 4~5배차이까지 남 앞의 구조에서는 시계열구조가 시간순서로 비슷한 위치에 저장되어있고
        #후자로 유추해 보아 내부적인 최적화 저장 방식을 사용하는 것으로 알게됨 또한 기존 데이터용량이 전번은 1/2배압축되고
        #후자는 기존문자열로 구성된 데이터에서 데이트타임+더블+롱등의 구조로 바꾸면서 약 3.4배 가량 늘었지만 압축된 데이터는 1/3배 압축되었음 이는 0.333배정도 압축효율이 좋음음
    except Exception as e:
        print("find_stock_data_list : "+e)
    finally :
        client.close()
def change_field_name():
    try :
        client = connect_mongo()
        collection2 = client["trader"]["stock_ts"]
        print("start find symbs")
        # 1. 모든 symb 값 조회
        symbs = collection2.distinct('symb')
        print("finded symbs")
        print(symbs)
        # 2. 각 symb마다 updateMany 실행
        # for symb in symbs:
        #     result = collection2.update_many(
        #         {'symb': symb},  # metaField로만 쿼리
        #         {'$rename': {'clos': 'close'}}
        #     )
        #     print(f"symb: {symb}, modified: {result.modified_count}")

    except Exception as e:
        print("find_stock_data_list : "+e)
    finally :
        client.close()

# def find_stock_ts_recent_count(symb,count):
#     try :
#         client = connect_mongo()
#         collection = client["trader"]["stock_ts"]
#         isClos = collection.find({"symb":symb},{"clos":1,"_id":1}).sort('timestamp',-1).limit(1)
#         if(isClos.next().get("clos") is None):
#             print("변수에 clos가 없습니다 return.")
#             return
#         else:
#             print("변수에 clos가 있습니다.")

#         result = collection.find({"symb": symb},{"_id":1},).sort('timestamp',-1).limit(count)
#         docs = list(result)  # 커서 전체를 리스트로 변환
#         print(docs)
#         if docs:
#             first_key = result[0]
#             if first_key == "clos":
#                 print("첫 번째 필드명이 'clos'입니다.")
#             else:
#                 print("첫 번째 필드명이 'clos'가 아닙니다.")
#         else:
#             print("결과가 없습니다.")
#         id_list = [doc["_id"] for doc in result]
#         return id_list
#     except Exception as e:
#         print("find_stock_data_list : "+e)
#     finally :
#         client.close()

#필드명이 clos인 도큐먼트 삭제제
def remove_illegalprops_documents():
    try :
        client = connect_mongo()
        collection = client["trader"]["stock_ts"]
        end_time = datetime(2025, 5, 4)
        start_time = datetime(2025, 3, 20)

        delete_result = collection.delete_many({
            "timestamp": {"$gte": start_time, "$lt": end_time}
        })

        print(f"Deleted {delete_result.deleted_count} documents.")

    except Exception as e:
        print("find_stock_data_list : "+e)
    finally :
        client.close()    

    
