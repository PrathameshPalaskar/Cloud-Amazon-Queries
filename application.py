__author__ = 'Shree'

from flask import Flask, render_template, request, redirect, url_for, send_from_directory
#from werkzeug import secure_filename
import  MySQLdb,csv,random,datetime
import os

import boto
from boto.s3.key import Key
import memcache
import hashlib




#import pandas as pd

global fi

application = Flask(__name__)  # Change assignment here

AWS_ACCESS_KEY = 'AKIAI5BZZDMS4GJOTT3A'
AWS_ACCESS_SECRET_KEY = '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'

'''conn = boto.rds.connect_to_region(
     "us-west-2",
     aws_access_key_id=AKIAI5BZZDMS4GJOTT3A,
    aws_secret_access_key=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)'''

bucket_name = 'prathamesh15'
conn = boto.connect_s3(AWS_ACCESS_KEY,AWS_ACCESS_SECRET_KEY)
bucket = conn.create_bucket(bucket_name, location=boto.s3.connection.Location.DEFAULT)

#create memcache connection
def mc_con():
    mc = memcache.Client(['assmemcache.y26s7w.cfg.usw2.cache.amazonaws.com:11211'], debug=1)
    print "Memcache connection created"
    return mc


#conn = boto.rds.connect_to_region(    "us-west-2",aws_access_key_id=AKIAI5BZZDMS4GJOTT3A,aws_secret_access_key=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)

def condb():
    db = MySQLdb.connect(user='prathamesh',passwd='Pratham1',host='aa1q5l6bdmkwloc.cbzchznvjkzn.us-west-2.rds.amazonaws.com',db='ebdb')
    return db

@application.route("/")        # Change your route statements
def index():
    return render_template('upload.html')


#upload the file to the ec2 instance
@application.route('/upload', methods=['POST'])

def upload():


    a=condb()
    cur=a.cursor()

    file = request.files['file_upload']
    file_name = file.filename
    print "File Name: "+file_name
    #file_name = secure_filename(testfile)
    print 'Uploading %s to Amazon S3 bucket %s' % \
    (file_name, bucket_name)
    content = file.read()
    k = Key(bucket)
    k.key = file_name
    starttime=datetime.datetime.now()
    #starttime=time.clock()
    k.set_contents_from_string(content)
    #endtime=time.clock()
    endtime=datetime.datetime.now()
    res = endtime-starttime
    print "File Uploaded"


    # Rewind for later use
    file.seek(0)

    '''try:
        with open(file_name.filename, newline='') as f:
            csvreader1=csv.reader(f)
            row1=next(csvreader1)
    except StopIteration:
            print "exception"'''

    #
    # data = pd.read_csv(file_name.filename, nrows=1)
    '''try:
        with open(file_name.filename,'w+') as gu:
         r = csvreader.next()
       # line1=r.next()
    except:
        print "Exception"'''

    with open(file.filename) as f:
        reader = csv.reader(f)
        row1 = next(reader)

    global fi
    fi = file.filename[:-4]
    sql2="drop table if exists "+fi
    insert_str = "LOAD DATA LOCAL INFILE '" + file.filename + "' INTO TABLE " + fi + " FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' IGNORE 1 LINES"
    cur.execute(sql2)

    createtable_time=datetime.datetime.now()
    query="Create table if not exists "+ fi+ "("
    for i in range(0,len(row1)):
        query+=row1[i]+" varchar(30), "
    #query ="Create table UNPrecip ( CountryTerritory varchar(30),StationName varchar(30),WMOStationNumber varchar(30),Unit varchar(30),Jan varchar(30),Feb varchar(30),, id int(6) auto_increment primary key(id))"'''
    query ="Create table UNPrecip ( CountryTerritory varchar(30),StationName varchar(30),WMOStationNumber int(7),Unit varchar(10),Jan decimal(" \
           "7,2), February decimal(7,2), March decimal(7,2),April decimal(7,2),May decimal(7,2),June decimal(7,2),July decimal(7,2),August decimal(7,2),Sep decimal(7,2),October decimal(7,2),Nov decimal(7,2),December decimal(6,2), "
    query+=" id int auto_increment primary key)"
    cur.execute(query)
    endcreate_time=datetime.datetime.now()
    total_create=endcreate_time-createtable_time
    print "Table created "+fi


    print " time taken is for table creation " + str(total_create)


    with open (file.filename,'w') as file2:
        file2.write(file.read())
    print file2

    #sqllocal="LOAD DATA LOCAL INFILE '" + file_name.filename+ "' INTO TABLE "+ file_name.filename[:-4]+"FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' ignore 1 lines"
    #print sqllocal
    cur.execute(insert_str)

    sql_delete="Delete from UNPrecip where Jan >=10000.00 or February>= 10000.00 or March >=10000.00"
    str_sql_delete_time=datetime.datetime.now()
    res_delete=cur.execute(sql_delete)
    end_sql_delete_time=datetime.datetime.now()
    total_sql_delete=end_sql_delete_time-str_sql_delete_time
    print "working"+str(res_delete)

    sql_count_tuples="Select count(*) from UNPrecip"
    cur.execute(sql_count_tuples)
    result=cur.fetchone()

    a.commit()
    cur.close()
    a.close()

    #return "Time for table creation"+str(total_create)+" Time for deletion queries"+str(total_sql_delete)+ " The total tuples remaining now "+str(result)

    return render_template('randomquery.html')

#random query without the mecache part
@application.route('/randomquery', methods=['POST'])
def randomquery():
    global fi
    print fi
    a=condb()
    cur=a.cursor()

    b=int(request.form.get('number_queries'))
    c=int(request.form.get('number_tuples'))


    str_time=datetime.datetime.now()

    '''obj=mc.get(random_integer1)
    if not obj:

        for num in range(1,b):
            cur=a.cursor()
            random_integer=random.randint(1,90000)
            sql_select="Select * from %s where id = %s  and id<= %s" %(fi,random_integer,c)
        #query_param(fi,random_integer,c)
            cur.execute(sql_select)
            res=cur.fetchone()
            mc.set(random_integer,res)'''

    for i in range(1,b):
        #mc.flush_all()

        random_integer=random.randint(1,12)
        sql_select=("Select * from {0} where period = {1} ").format(fi,random_integer)
        print sql_select
        cur.execute(sql_select)

    end_time=datetime.datetime.now()
    total_time=end_time-str_time
    a.close()
    return "Without Caching the time is "+str(total_time)

#Random query with the memcache
@application.route('/randomquery_mem', methods=['POST'])
def randomquery_mem():
    global fi
    print fi
    a=condb()
    mc=mc_con()
    cur=a.cursor()
    c_b=int(request.form.get('cache_queries'))
    c_c=int(request.form.get('cache_tuples'))

    str_time=datetime.datetime.now()

    #mc.flush_all()
    for num in range(1,c_b):

        random_integer=random.randint(1,12)

        sql_select=("Select * from {0} where period = {1} ").format(fi,random_integer)
        hash_string=sql_select.replace(" ","")
        hash_key=hashlib.sha256(hash_string)
        hash_obj=hash_key.hexdigest()
        val = mc.get(hash_obj)
        if val is None:
            print("work")
            cur.execute(sql_select)
            result = cur.fetchone()
            status = mc.set(hash_obj,result)

            print "status of Value inserted to memcache: "+str(status)
        else:
			print("Loaded from Memcached")
			#print "Value from memcache: "+ str(val)
        #query_param(fi,random_integer,c)


    end_time=datetime.datetime.now()
    total_time=end_time-str_time
    a.close()
    return "With Caching the time is "+str(total_time)


#Random query with dynamic input that is query directly from user
@application.route('/quiz_dynamic', methods=['POST'])
def quiz_dynamic():
    global fi
    print fi
    a=condb()
    cur=a.cursor()

    b=(request.form.get('dynamic_wo_cache'))
    for i in range(1,200):
        #mc.flush_all()

        random_integer=random.randint(1,12)
        #sql_select=("Select * from {0} where period = {1} ").format(fi,random_integer)
        sql_select=b
        print sql_select
        cur.execute(sql_select)
    a.close()
    return "Without Caching the output "






if __name__ == "__main__":
    application.run(debug=True)



#delete files from a bucket
'''

b = Bucket(conn, S3_BUCKET_NAME)

k = Key(b)

k.key = 'images/my-images/'+filename

b.delete_key(k) '''