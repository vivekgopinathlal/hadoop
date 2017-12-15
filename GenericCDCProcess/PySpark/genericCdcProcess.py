from __future__ import print_function
import os, sys, csv, logging, argparse
sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0])))
sys.path.append("<<Local Path>>/conf")
import Conf

from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext, SparkSession
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType, DoubleType

def setup_logger(logger_name, log_file, level=logging.INFO):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    fileHandler = logging.FileHandler(log_file, mode='w')
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)

    l.setLevel(level)
    l.addHandler(fileHandler)
    l.addHandler(streamHandler)
def printAval(flg,msgPart):
    if flg:
       msg=msgPart+" validated Successfully \n"
    else:
       msg=msgPart+" failed, Kindly validated \n"

    return msg

def validateColumns(productsHCBase, productsHCStg, myArgs):
    base_df=productsHCBase.columns
    delta_df=productsHCStg.columns
    flg=""
    msg=""
    attrList=set(base_df).intersection(set(delta_df))
    flg=len(attrList) == len(base_df) & len(attrList) == len(delta_df)
    if flg:
       msg="Base Table Attr in line with Delta Table Attr : \n"
    else:
       msg="Mismatch in the attribute list between  Base Table and Delta Table \n"

    uflag=len(set(myArgs.updateAttr).intersection(set(attrList)))== len(myArgs.updateAttr)
    msg=msg+printAval(uflag,"Update Attributes")
    pflag=len(set(myArgs.partAttr).intersection(set(attrList)))== len(myArgs.partAttr)
    msg=msg+printAval(pflag,"Partition Attributes")
    kflag=len(set(myArgs.key).intersection(set(attrList)))== len(myArgs.key)
    print(set(myArgs.key).intersection(set(attrList)))
    msg=msg+printAval(kflag,"Key Attributes")

    return flg,msg,attrList

def diffRec(rec1,rec0):
    flg='UC'
    cnt=0
    recDisp=rec0
    #for rec1Data in rec1.split(","):
    for rec1Data in rec1:

        if(rec1Data != rec0[cnt]):
            #flg='Attr:'+str(cnt) +','+flg
            flg='U'
            recDisp=rec1

        cnt=cnt+1
    return flg,recDisp

def main(sc, hc, args):

        #Initialize the variables
        entity=args.entity
        baseTable=args.baseTable
        deltaTable=args.deltaTable
        dataBase=args.db
        uattr=args.updateAttr
        pattr=args.partAttr
        keys=args.key

        setup_logger(entity+'_log', Conf.HDP_COMMON_DATA_LOG +'/CDC_Info.log',logging.INFO)
        setup_logger(entity+'_debug', Conf.HDP_COMMON_DATA_LOG +'/CDC_Debug.log',logging.DEBUG)
        setup_logger(entity+'_stats', Conf.HDP_COMMON_DATA_LOG +'/CDC_Stats.log')

        infoLog = logging.getLogger(entity+'_log')
        debugLog = logging.getLogger(entity+'_debug')
        statsLog = logging.getLogger(entity+'_stats')

        print(args)
        print(entity,baseTable,deltaTable,dataBase,uattr,pattr,keys)
        hc.sql("use "+dataBase)
        hc.sql("drop table IF EXISTS "+dataBase+".temptable_"+deltaTable)
        productsHCBase=hc.sql("select product_id, product_category_id, product_name, product_description, product_price, product_image from "+dataBase+"."+baseTable)
        productsHCStg=hc.sql("select product_id, product_category_id, product_name, product_description, product_price, product_image from "+dataBase+"."+deltaTable)
        #Still workin on the below validation
        flg,msg,attrList=validateColumns(productsHCBase, productsHCStg, args)
        print("Flag :",flg)
        print("Message :", msg)
        print(attrList)
        quit()
        productsBaseRDD=productsHCBase.rdd
        productsRDD=productsHCStg.rdd
        infoLog.info("RDD - read operation completed for productsBase and products dataset")

        #Key Value Pair of the datasets
        kv_base=productsBaseRDD.map(lambda brec: ((brec.product_id,brec.product_category_id),brec))
        kv_new=productsRDD.map(lambda nrec: ((nrec.product_id,nrec.product_category_id),nrec))
        infoLog.info("Key Value Pair has been created")

        #Full Outer Join
        fullOuterJn_base_new=kv_base.fullOuterJoin(kv_new)
        fullOuterJn_base_new.map(lambda rec: (int(rec[0][0]),rec)).sortByKey(False).collect()
        infoLog.info("Performing fullouter join Base on Delta/New")

        #Identify the Inserts, Deletes & Updates
        insertRec_base_new=fullOuterJn_base_new.filter(lambda rec: rec[1][0]==None).map(lambda rec: ("I",rec[1][1]))
        infoLog.info("Identified the Insert Records & captured in Debug Log : "+Conf.HDP_COMMON_DATA_LOG +'/CDC_Debug.log')
        debugLog.debug("Following are the list of records identified for Insert : \n %s",insertRec_base_new.collect())

        deleteRec_base_new=fullOuterJn_base_new.filter(lambda rec: rec[1][1]==None).map(lambda rec: ("D",rec[1][0]))
        infoLog.info("Identified the Records to be Deleted")
        debugLog.debug("Following are the list of records identified for deletion : \n %s",deleteRec_base_new.collect())

        possibleChangesRec=fullOuterJn_base_new.filter(lambda rec: (rec[1][0]!=None and rec[1][1]!=None)).map(lambda rec: rec[1])
        infoLog.info("Identified the possible Update and UnChanged data")

        infoLog.info("Mark the Updates and UnChanged records")
        updateRec_base_new=possibleChangesRec.map(lambda rec: diffRec(rec[1],rec[0]))
        debugLog.debug("Following is the list of records identified to be updated : \n %s ",updateRec_base_new.filter(lambda rec: rec[0]=='U').collect())

        #Identify the distinct partitions in update file
        identifyPartitions=updateRec_base_new.filter(lambda urec: urec[0]=='U').map(lambda rec: rec[1][1])
        partitionLists=sorted(set(identifyPartitions.collect()))
        debugLog.debug("Identified the distinct Partitions %s", partitionLists)

        #df=insertRec_base_new.union(updateRec_base_new).map(lambda x : x[1]).toDF()
        myrdd=insertRec_base_new.union(updateRec_base_new.filter(lambda rec: rec[1][1] in partitionLists))
        infoLog.info("Combined both Insert and Update records")

        #Identify the distinct partitions in update file
        identifyPartitions=myrdd.filter(lambda urec: urec[0]=='U' or urec[0]=='I' ).map(lambda rec: rec[1][1])
        partitionLists=sorted(set(identifyPartitions.collect()))
        debugLog.debug("Identified the distinct Partitions %s", partitionLists)


        statsLog.info("Base Table - Products : # of records before applying latest CDC data :- %s", productsBaseRDD.count())

        #reading the schema
        #product_id, product_category_id, product_name, product_description, product_price, product_image
        structInfo = StructType([StructField("product_id", IntegerType(), True),
        StructField("product_category_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("product_description", StringType(), True),
        StructField("product_price", DoubleType(), True),
        StructField("product_image", StringType(), True)])
        df=hc.createDataFrame(myrdd.map(lambda x : x[1]),structInfo)
        df.collect()
        df.write.partitionBy("product_category_id").saveAsTable(dataBase+".temptable_"+deltaTable)
        infoLog.info("Created a temp partition table in same db")
        debugLog.debug('df.write.partitionBy("product_category_id").saveAsTable(dataBase+".temptable_"+deltaTable)')

        dpHDP="use "+dataBase+";\n"
        epHDP="\n"
        with open('hive-cdc.sql','a+')as f:
         for part in partitionLists:
          dp="ALTER TABLE "+baseTable+" DROP IF EXISTS PARTITION (product_category_id="+str(part)+"); \n\n"
          dpHDP=dpHDP+dp
          f.write(dpHDP)
          dpHDP=""
          #hc.sql(dp)
          debugLog.debug('Executed Drop Partition : %s ',dp)
          infoLog.info('Drop Partition completed for %s partition',part)
          ep="ALTER TABLE "+baseTable+" EXCHANGE PARTITION (product_category_id="+str(part)+") WITH TABLE temptable."+deltaTable+"; \n\n"
          epHDP=ep+epHDP
          f.write(ep)
          #hc.sql(ep)
          debugLog.debug('Executed Exchane Partition : %s ', ep)
          infoLog.info('Exchange Partition completed for %s partition',part)
         f.read()
         f.close()
        #hc.sql("drop table IF EXISTS bbiuserdb.temptable")

        sc.parallelize(dp.split("\n")).saveAsTextFile("hive-cdc-dp.sql")
        sc.parallelize(epHDP.split("\n")).saveAsTextFile("hive-cdc-ep.sql")

        #infoLog.info('===================================== Performing Hive QL to exchange Partition ================================')
        #hc.sql(open("hive-cdc-dp.sql").read())
        #hc.sql(open("hive-cdc-ep.sql").read())
        #hc.sql(open("hive-cdc.sql").read())
        #infoLog.info('===================================== Completed Kindly verify the partitions   ================================')

        #Collecting the stats
        statsLog.info("Total number of records : ")
        statsLog.info("         to be inserted : %s", insertRec_base_new.count())
        statsLog.info("         to be updated  : %s", updateRec_base_new.filter(lambda rec: rec[0]=='U').count())
        statsLog.info("         to be deleted  : %s", deleteRec_base_new.count())

def parseArguments(parser):
    # Create argument parser
    #parser = argparse.ArgumentParser()

    # Positional mandatory arguments
    parser.add_argument("-b", "--baseTable", help="BaseTable")
    parser.add_argument("-d", "--deltaTable", help="Delta Table")
    parser.add_argument("-e", "--entity", help="Entity Name")
    parser.add_argument("-k", "--key", help="Key list")
    parser.add_argument("-u", "--updateAttr", help="Identifies if any changes are on these attributes")
    parser.add_argument("-p", "--partAttr", help="Attributes on which BaseTable is partitioned")

    # Optional arguments
    parser.add_argument("-s", "--db", help="Default Schema", default='bbiuserdb')

    # Print version
    parser.add_argument("--version", action="version", version='%(prog)s - Version 1.0')

    # Parse arguments
    args = parser.parse_args()

    return args

if __name__ == '__main__':
    # Parse the arguments
    parser = argparse.ArgumentParser("Perform CDC between Base and Staging table")
    args = parseArguments(parser)

    # Raw print arguments
    print("You are running the script with arguments: ")
    print(args.__dict__)
    for a in args.__dict__:
        if args.__dict__[a]:
           print("Values provided for : "+ str(a) + ": " + str(args.__dict__[a]))
        else:
           parser.print_help()
           sys.exit(0)
    sc = SparkContext()
    hc = SparkSession.builder.enableHiveSupport().getOrCreate()
    main(sc, hc, args)

