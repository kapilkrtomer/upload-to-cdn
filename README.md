upload-to-cdn
=============


import MySQLdb
import os 
from code1_first_junglee import home_page_yabhi
import logging 
import req_proxy 
from random import choice
import boto
from boto.s3.key import Key
import multiprocessing
import time
import sys


logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] (%(threadName)-10s) %(message)s')


class upload_to_cdn(home_page_yabhi):
    def __init__(self, dbusername, dbpassword, directory, databasename, databasetable, link):
        self.dbusername = dbusername
        self.dbpassword  = dbpassword

        image_directory = "%s/img_dir" %(directory)

        try:
            os.makedirs(image_directory)
        except:
            pass

        self.image_directory  = image_directory
        self.databasename = databasename
        self.databasetable = databasetable
      
        self.f = f = open("/home/desktop/proxy_http_auth.txt")
        
        self.pass_ip_list =  pass_ip_list = f.read().strip().split("\n")

        self.sku_list = []
       
        home_page_yabhi.__init__(self, directory, link)


    def __del__(self):
        self.db.close()
        self.f.close()


    def connect_to_db(self):
        self.db = db = MySQLdb.connect("localhost", self.dbusername, self.dbpassword, self.databasename)
        self.cursor = cursor = db.cursor()



    def extract_sku_image_link(self):
        cursor = self.cursor
        sql = """select  product_id, image_link, sort_image_link from %s where  upload_image_status = "NO" and status = "A" """
        sql = sql %(self.databasetable)
    
        cursor.execute(sql)
        self.results = results = cursor.fetchall()
       # print results


    def loop_over_result(self, line):
        sku = line[0]
        img_link = line[1]
        #img_link = img_link.replace("////","//")
        #print img_link
        sort_img_key = line[2]
        page = req_proxy.main(img_link)

        #print page
        filename = "%s/%s" %(self.image_directory, sort_img_key)
        file1 = open("/home/desktop/anit/zivame/success_image.txt","a+")
        f  = open(filename, "wb+")
        f.write(page)
       
        f.close()

        pass_ip = choice(self.pass_ip_list)
        
        user_pass_ip_port = pass_ip.split("@")
        proxy_user , proxy_pass = tuple(user_pass_ip_port[0].split(":"))
        proxy, proxy_port = tuple(user_pass_ip_port[1].split(":"))
        

        self.to_cdn(sku, filename, sort_img_key, proxy, proxy_port,  proxy_user,  proxy_pass)
        
        file1.write(str(sku)+"\n")
      
        
    
    def to_cdn(self, sku, filename, keyname, proxy, proxy_port,  proxy_user,  proxy_pass):
        #boto.set_stream_logger('boto')
        #c = boto.connect_s3()
        #c = boto.connect_s3(proxy ="69.12.72.105", proxy_port="80",  proxy_user="sun",  proxy_pass="india123")
        c = boto.connect_s3(proxy = proxy, proxy_port=proxy_port,  proxy_user=proxy_user,  proxy_pass=proxy_pass)
        b = c.get_bucket("zovon")
        k = Key(b)
        k.key = "prod_img/%s" %(keyname)
        k.set_metadata("Content-Type", 'image/jpeg')
        k.set_contents_from_filename(filename)
        k.make_public()
        c.close()
        self.sku_list.append(sku)
        os.remove(filename)



    def database_update_for_image(self):
        sku_range = ",".join([ '"%s"' %(sku )for sku in self.sku_list ])
        sql = """ update %s set upload_image_status = "YES" where product_id in (%s) """ 
        sql = sql %(self.databasetable, sku_range)
        
        
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except:
           self.db.rollback()
    
        
        
def supermain(directory):
    link = "http://www.junglee.com/site-directory/ref=footer_menu_all"
    databasename = "zivame"
    databasetable = "zivame_data"
    dbusername =  "root"
    dbpassword = "6Tresxcvbhy"
    obj = upload_to_cdn(dbusername, dbpassword, directory, databasename, databasetable, link)
    obj.connect_to_db()
    obj.extract_sku_image_link()
   
    
    length_main = len(obj.results)
    obj.results = obj.results

    len_div = length_main/5
    len_mod = - (length_main%5)
    
    jobs = []

    for length in xrange(1, 6):
        len_str = len_div * (length -1)
        len_end = len_div * (length)

        j = multiprocessing.Process(target=obj.que_thread(obj.testing_output, obj.results[len_str : len_end], obj.loop_over_result))
        jobs.append(j)
        j.start()
        


    j = multiprocessing.Process(target=obj.que_thread(obj.testing_output, obj.results[len_mod:], obj.loop_over_result))
    jobs.append(j)
    j.start()


    for j in jobs:
        j.join()

    obj.que_thread(obj.testing_output, obj.results[-2:], obj.loop_over_result)
    obj.database_update_for_image()
    

if __name__=="__main__":
    directory = "/home/desktop/anit/zivame/zivame_image"
    supermain(directory)
