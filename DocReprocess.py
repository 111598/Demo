import pymongo
from pymongo import MongoClient
import csv
import os
import logging
from bson.objectid import ObjectId
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium import webdriver
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders
from selenium.webdriver.common.keys import Keys

# Owned
__author__ = "Velkumar L"
__credits__ = "App Support Team"
__license__ = "Merrill Corp."
__version__ = "2.01"
__maintainer__ = "App Support"
__email__ = "velkumar.loganathan@datasite.com"
__status__ = "Dev"
__file__ = "DocReprocess.py"


# # For .EXE conversion with icon => pyinstaller -i "C:\icon\creativity.ico" -F "C:\Users\VLoganathan\PycharmProjects\Reprocess\DocReprocess.py"

# sending mail via SMTP server

def send_mail(send_from, send_to, subject, text, files=[], server="relay.stp.mrll.com"):
    assert type(send_to) == list
    assert type(files) == list
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    msg.attach(MIMEText(text))
    for f in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(f, "rb").read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
        msg.attach(part)
    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()


def main():
    global failure
    logging.info('processing Main Function')
    for num in range(2):
        reprocess(username[num], password[num], host[num], dbc[num], usrdbname, usrdbpwd, usrdbhost, swagger_URL[num])
    if failure:
        esubject = "FAILURE Doc Reprocess"
        esend_to = ["velkumar.loganathan@datasite.com"]
        if len(file_list) == 0:
            ebody = "Error on the process of Pushing data" + "\n" + "\n" + "Datacenter  " + dbc + "\n" + "\n" + "Please find the following Error" + "\n" + str(
                error)
            send_mail(send_from, esend_to, esubject, ebody, file_list)
        elif len(file_list) > 0:
            ebody = "Error on the process of Pushing data" + "\n" + "\n" + "Datacenter  " + dbc + "\n" + "Attachment contain the Docs details. Please do reprocess Manually." + "\n" + "\n" + "Please find the following Error" + "\n" + str(
                error)
            send_mail(send_from, esend_to, esubject, ebody, file_list)
    else:
        pass


#
# Getting Document metadata Ids and Reprocessing via Swagger End Points using Selenium Web Driver
#
def reprocess(username, password, host, dbc, usrdbname, usrdbpwd, usrdbhost, swagger_url):
    pass
    global demopro
    global clientpro
    global demo_docs
    global client_docs
    global us_process
    global eu_process
    global not_process
    global pre_content
    global contents
    global zero_Docs
    global failure
    global error
    try:
        # if client.get_database('documentProcessing') and client.get_database('docMetadata') and usclient.get_database(
        #         'projects'):
        url = 'mongodb://{0}:{1}@{2}:27017/admin'.format(username, password, host, usrdbname, usrdbpwd, usrdbhost)
        usrurl = 'mongodb://{0}:{1}@{2}:27017/admin'.format(usrdbname, usrdbpwd, usrdbhost)
        usclient = MongoClient(usrurl)
        usrdb = usclient.get_database('projects')
        usrcol = usrdb.details
        client = MongoClient(url)
        db = client.get_database('documentProcessing')
        col = db.task
        mdb = client.get_database('docMetadata')
        mcol = mdb.metadata
        listofIds = []
        logging.info('Connected to the Database' + dbc)
        logging.info('Grabbing the Docs and Project Ids')
        result = col.find({"state": {"$in": ["REQUIRES_MANUAL_PROCESSING", "PROCESSING"]}})
        for docs in result:
            value = ObjectId(str(docs['metadataId']))
            listofIds.append(value)
        mresult = mcol.find(
            {"status": "PROCESSING", "createDate": {"$gte": yesterday, "$lte": today},
             "_id": {"$not": {"$in": listofIds}}},
            {"_id": 1, "projectId": 1, "createDate": 1, "docForms.docProperties.fileType": 1})
        data = list()
        list_id = list()
        demo = list()
        clist = list()
        ddocs = 0
        cdocs = 0
        pdf = 0
        xlsx = 0
        docx = 0
        file_csv = 0
        file_zip = 0
        txt = 0
        xod = 0
        pptx = 0
        xls = 0
        png = 0
        jpg = 0
        file_tif = 0
        file_doc = 0
        file_name = folder_path + '/' + dbc + '_Reprocess.csv'
        file_list.append(file_name)
        for doc in mresult:
            doc_id = str(doc['_id'])
            data.append(doc)
        #
        #  Creating the file in csv format based on Database Center with id, projectId, createDate
        #
        logging.info('Creating the ' + dbc + '.csv file')
        with open(file_name, 'w', encoding="Utf8", newline='') as outfile:
            fields = ['_id', 'projectId', 'createDate', 'fileType', 'DB']
            write = csv.DictWriter(outfile, fieldnames=fields)
            write.writeheader()
            for x in data:
                write.writerow(
                    {'_id': str(x['_id']), 'projectId': str(x['projectId']), 'createDate': str(x['createDate']),
                     'fileType': str(x['docForms'][0]['docProperties']['fileType']), 'DB': str(dbc)})
        print("Chounting File Type")
        for i in data:
            ftype = str(i['docForms'][0]['docProperties']['fileType'])
            print(str(i['docForms'][0]['docProperties']['fileType']))
            if ftype in ["png", ".png"]:
                png += 1
            elif ftype in ["xlsx", ".xlsx"]:
                xlsx += 1
            elif ftype in ["csv", ".csv"]:
                file_csv += 1
            elif ftype in ["docx", ".docx"]:
                docx += 1
            elif ftype in ["txt", ".txt"]:
                txt += 1
            elif ftype in ["zip", ".zip"]:
                file_zip += 1
            elif ftype in ["xod", ".xod", "XOD"]:
                xod += 1
            elif ftype in ["pptx", ".pptx"]:
                pptx += 1
            elif ftype in ["xls", ".xls"]:
                xls += 1
            elif ftype in ["pdf", ".pdf"]:
                pdf += 1
            elif ftype in ["doc", ".doc"]:
                file_doc += 1
            elif ftype in ["jpg", ".jpg"]:
                jpg += 1
            elif ftype in ["tif",".tif"]:
                file_tif += 1
            else:
                print('File type is not Match' + str(i['docForms'][0]['docProperties']['fileType']))
        print('Printing the statement')
        print(str('pdf' + str(pdf) + ' xlsx' + str(xlsx) + ' csv' + str(file_csv) + ' docx' + str(docx) +
                  ' zip' + str(file_zip) + ' xod' + str(xod) + ' pptx' + str(pptx) + ' xls' + str(xls)) +
              ' png' + str(png) + ' jpg' + str(jpg) + ' doc' + str(file_doc) + ' tif' + str(file_tif))

        for y in data:
            pro_id = ObjectId(str(y['projectId']))
            list_id.append(pro_id)
            final = usrcol.find(
                {"_id": pro_id},
                {"projectInfo": 1, "deleted": 1})
            #
            #   Checking the Document related to Client project or Demo project
            #
            for item in final:
                demo_value = item['projectInfo']['demo']
                if demo_value == False:
                    if item['_id'] not in demo:
                        demo.append(item['_id'])
                        ddocs += 1
                    elif item['_id'] in demo:
                        ddocs += 1
                elif demo_value == True:
                    if item['_id'] not in clist:
                        clist.append(item['_id'])
                        cdocs += 1
                    elif item['_id'] in clist:
                        cdocs += 1
                else:
                    print(demo_value)
        print(dbc + ' Process Details:')
        print("==================================")
        #
        #   Creating Detail file for mail body
        #
        logging.info('Creating details.txt file for the information about the process')
        dfile = open(path + 'details.txt', 'a')
        dfile.write(dbc + ' Process Details:' + '\n'
                    + "==================================" + '\n'
                    + 'Demo Project : ' + str(len(demo)) + '\n'
                    + 'Demo Docs : ' + str(ddocs) + '\n'
                    + 'Client Project : ' + str(len(clist)) + '\n'
                    + 'Client Docs :' + str(cdocs) + '\n'
                    + "csv = " + str(file_csv) + "; " + "\t"
                    + "docx = " + str(docx) + "; " + "\t"
                    + "doc = " + str(file_doc) + "; " + "\t"
                    + "pdf = " + str(pdf) + "; " + "\t"
                    + "pptx = " + str(pptx) + "; " + "\t"
                    + "txt = " + str(txt) + "; " + "\n"
                    + "xod = " + str(xod) + "; " + "\t"
                    + "xls = " + str(xls) + "; " + "\t"
                    + "xlsx = " + str(xlsx) + "; " + "\t"
                    + "png = " + str(png) + "; " + "\t"
                    + "tif = " + str(file_tif) + "; " + "\t"
                    + "jpg = " + str(jpg) + "; " + "\t"
                    + "zip = " + str(file_zip) + "; " + '\n')
        dfile.close()
        cfile = open(path + 'count.txt', 'a')
        cfile.write(str(len(demo) + ddocs + len(clist) + cdocs))
        cfile.close()
        readf = open(path + 'count.txt', 'r')
        if readf.mode == "r":
            contents = readf.read()
            print(' Today count check: ' + contents)
        #
        #   Document metadata count
        #
        if os.path.exists(pre_path) and os.path.exists(file):
            pre_read = open(pre_path + 'count.txt', 'r')
            if pre_read.mode == "r":
                pre_content = pre_read.read()
                if pre_content == contents:
                    if pre_content == '00' or pre_content == 00 and contents == '00' or contents == 00:
                        zero_Docs = True
        print(
            "* Demo Project:  " + str(len(demo)) + "\n" + "* Demo_Docs :" + str(ddocs) + "\n" + "* Client :" + str(
                len(clist)) + "\n" + "* Client_Docs :" + str(cdocs))
        demopro = len(demo)
        clientpro = len(clist)
        demo_docs = ddocs
        client_docs = cdocs
        if dbc == 'US':
            usstr = "US DOC Process" + "\n" + "==============" + "\n" + "Demo Project:  " + str(
                demopro) + "\n" + "Demo_Docs: " + str(
                demo_docs) + "\n" + "Client: " + str(clientpro) + "\n" + "Client_Docs: " + str(client_docs) + "\n" + \
                    "csv = " + str(file_csv) + "; " + "\t" + \
                    "docx = " + str(docx) + "; " + "\t" + \
                    "doc = " + str(file_doc) + "; " + "\t" + \
                    "pdf = " + str(pdf) + "; " + "\t" + \
                    "pptx = " + str(pptx) + "; " + "\t" + \
                    "txt = " + str(txt) + "; " + "\n" + \
                    "xod = " + str(xod) + "; " + "\t" + \
                    "xls = " + str(xls) + "; " + "\t" + \
                    "xlsx = " + str(xlsx) + "; " + "\t" + \
                    "png = " + str(png) + "; " + "\t" + \
                    "tif = " + str(file_tif) + "; " + "\t" + \
                    "jpg = " + str(jpg) + "; " + "\t" + \
                    "zip = " + str(file_zip) + "; "
            us_process = usstr
            print(us_process)
        elif dbc == 'EU':
            esstr = "EU DOC Process" + "\n" + "==============" + "\n" + "Demo Project:  " + str(
                demopro) + "\n" + "Demo_Docs: " + str(
                demo_docs) + "\n" + "Client: " + str(clientpro) + "\n" + "Client_Docs: " + str(client_docs) + "\n" + \
                    "csv = " + str(file_csv) + "; " + "\t" + \
                    "docx = " + str(docx) + "; " + "\t" + \
                    "doc = " + str(file_doc) + "; " + "\t" + \
                    "pdf = " + str(pdf) + "; " + "\t" + \
                    "pptx = " + str(pptx) + "; " + "\t" + \
                    "txt = " + str(txt) + "; " + "\n" + \
                    "xod = " + str(xod) + "; " + "\t" + \
                    "xls = " + str(xls) + "; " + "\t" + \
                    "xlsx = " + str(xlsx) + "; " + "\t" + \
                    "png = " + str(png) + "; " + "\t" + \
                    "tif = " + str(file_tif) + "; " + "\t" + \
                    "jpg = " + str(jpg) + "; " + "\t" + \
                    "zip = " + str(file_zip) + "; "
            eu_process = esstr
            print(eu_process)
        if len(demo) == 0 and ddocs == 0 and len(clist) == 0 and cdocs == 0:
            print('No values')
            print("Demo Project:  " + str(len(demo)) + "\n" + "Demo_Docs: " + str(ddocs) + "\n" + "Client : " + str(
                len(clist)) + "\n" + "Client_Docs :" + str(cdocs))
        else:
            #
            #   After getting metadata Ids of docs. Reprocessing via swagger endpoint using selenium
            #
            logging.info('Processing Docs via Swagger EndPoint using Selenium starts')
            driver = webdriver.Chrome(
                executable_path="c:\\dev\\reprocess\\lib\\chromedriver.exe")
            driver.set_window_size(1024, 600)
            driver.maximize_window()
            website_URL = "https://datasiteone.merrillcorp.com/global/projects"
            driver.get(website_URL)
            time.sleep(2)
            driver.find_element_by_id("username").send_keys("111598")
            driver.find_element_by_id("password").send_keys("Lbsnaa20$")
            driver.find_element_by_class_name("primary").click()
            time.sleep(12)
            token = str(driver.execute_script('return window.localStorage.authToken'))
            time.sleep(2)
            driver.find_element_by_tag_name('body').send_keys(Keys.CONTROL + 't')
            website_URL = swagger_url
            driver.get(website_URL)
            time.sleep(6)
            print(token)
            sel = driver.find_element_by_id("select")
            for option in sel.find_elements_by_tag_name('option'):
                if option.text == 'Metadata':
                    option.click()
                    break
            time.sleep(2)
            driver.find_element_by_xpath('//*[@id="operations-tag-Documents"]/a').click()
            driver.execute_script("window.scrollTo(0,2000)")
            driver.find_element_by_id("operations-Documents-reprocessUsingPOST").click()
            time.sleep(2)
            driver.find_element_by_xpath(
                '//*[@id="operations-Documents-reprocessUsingPOST"]/div[2]/div/div[1]/div[1]/div[2]/button').click()
            time.sleep(2)
            driver.find_element(By.CSS_SELECTOR, ".parameters:nth-child(1) input").click()
            driver.find_element(By.CSS_SELECTOR, ".parameters:nth-child(1) input").clear()
            driver.find_element(By.CSS_SELECTOR, ".parameters:nth-child(1) input").send_keys(str('Bearer ' + token))
            time.sleep(2)
            driver.find_element(By.CSS_SELECTOR, ".parameters:nth-child(2) input").clear()
            print('Cleared projectId')
            time.sleep(2)
            driver.find_element(By.CSS_SELECTOR, ".body-param__text").click()
            driver.find_element(By.CSS_SELECTOR, ".body-param__text").clear()
            print('Clearing TextArea')
            time.sleep(2)
            try:
                for test_item in data:
                    logging.info('Processing docs and projects ids')
                    test_id = str(test_item['_id'])
                    test_projectId = str(test_item['projectId'])
                    print(
                        '================== Doc_Id ' + test_id + '  &  Project_Id ' + test_projectId + '=======================')
                    print('Entering projectId')
                    driver.find_element(By.CSS_SELECTOR, ".parameters:nth-child(2) input").send_keys(test_projectId)
                    javascript = '{"language":null,"metadataIds":["' + test_id + '"]}'
                    print('Value to be passed to the Text area')
                    print(javascript)
                    print('Clicking the TextArea')
                    driver.find_element(By.CSS_SELECTOR, ".body-param__text").click()
                    print('Now going to Clear the TextArea')
                    driver.find_element(By.CSS_SELECTOR, ".body-param__text").clear()
                    print('cleared TextArea')
                    time.sleep(1)
                    print('Clicking the TextArea')
                    driver.find_element(By.CSS_SELECTOR, ".body-param__text").click()
                    print('passing the value to TextArea')
                    driver.find_element(By.CSS_SELECTOR, ".body-param__text").send_keys(str(javascript))
                    time.sleep(1)
                    driver.find_element(By.CSS_SELECTOR, ".execute").click()
                    print('Executing')
                    time.sleep(1)
                    print('Clicking the ProjectId for Enter Next Value')
                    driver.find_element(By.CSS_SELECTOR, ".parameters:nth-child(2) input").clear()
                    print('Clicking the TextArea for Enter Next Value')
                    driver.find_element(By.CSS_SELECTOR, ".body-param__text").clear()
                    time.sleep(2)
                    code = driver.find_element(By.XPATH,
                                               '//*[@id="operations-Documents-reprocessUsingPOST"]/div[2]/div/div[3]/div[2]/div/div/table/tbody/tr/td[1]').text
                    if code != '200' or code != '201' or code != '202':
                        if code == '400' or code == '401' or code == '403' or code == '404' or code == '500':
                            not_process.append(
                                'Doc_id: ' + test_id + ', Project_id: ' + test_projectId + ', DB: ' + dbc + "\n")
                    print('=========================================')
                return True
            except Exception as e:
                logging.error(e)
                print(e)
                failure = True
                error = str(e)
                return False
            logging.info('Reprocess done successfully')
            print('Reprocess done successfully')
            print('Closing the Automation IDE Page')
            logging.info('Closing Driver')
            driver.close()
        return True
    except Exception as e:
        logging.error(e)
        print("Please check your connection")
        print(e)
        failure = True
        error = str(e)
        return False
    logging.info('Closing MongoClient')
    client.close()


if __name__ == '__main__':
    ctoday = datetime.today() - timedelta(hours=8)
    print('Today -> ' + ctoday.strftime('Day: %d Month: %m Year: %Y Time: %H:%M %p'))
    cyesterday = datetime.today() - timedelta(hours=48)
    print('Yesterday -> ' + cyesterday.strftime('Day: %d Month: %m Year: %Y Time: %H:%M %p'))
    now = datetime.now()
    today = datetime(int(ctoday.strftime('%Y')), int(ctoday.strftime('%m')), int(ctoday.strftime('%d')), int(ctoday.strftime('%H')), int(ctoday.strftime('%M')))
    yesterday = datetime(int(cyesterday.strftime('%Y')), int(cyesterday.strftime('%m')), int(cyesterday.strftime('%d')), int(cyesterday.strftime('%H')),
                         int(cyesterday.strftime('%M')))
    from_date = ctoday.strftime('%d/%m/%Y %H:%M %p')
    to_date = cyesterday.strftime('%d/%m/%Y %H:%M %p')
    folder = str(int(ctoday.strftime('%d'))) + '_' + str(int(ctoday.strftime('%m'))) + '_' + str(int(ctoday.strftime('%Y')))
    pre_folder = str(int(ctoday.strftime('%d')) - 1) + '_' + str(int(ctoday.strftime('%m'))) + '_' + str(int(ctoday.strftime('%Y')))
    pre_path = "c:/dev/reprocess/" + pre_folder + "/text/"
    path = "c:/dev/reprocess/" + folder + "/text/"
    log_path = "c:/dev/reprocess/" + folder + "/log/"
    folder_path = "c:/dev/reprocess/" + folder
    file = pre_path + "count.txt"
    try:
        # Create target Directory
        os.makedirs(path)
        os.makedirs(log_path)
        print("Directory ", path, " Created ")
        print("Directory ", log_path, " Created ")
    except FileExistsError:
        print("Directory ", path, " already exists")
        print("Directory ", log_path, " already exists")
    dbc = ["US", "EU"]
    username = ["readOnly", "readOnly_eu"]
    password = ["pr0dd3v", "pr0dd3v3u"]
    host = ["mgd-us2p-mgs-1a.dmz01.mrll.com", "mongodb-prod-eu4.mrll.com"]
    swagger_URL = ["https://datasiteone.merrillcorp.com/swagger-ui.html",
                   "https://global.datasiteone.merrillcorp.com/swagger-ui.html"]
    port = "27017"
    usrdbhost = "mgd-us2p-gd-1a.dmz01.mrll.com"
    usrdbname = "readOnly"
    usrdbpwd = "pr0dd3v"
    LOG_FILENAME = log_path + 'doc.log'
    logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
    file_list = list()
    demopro = 0
    demo_docs = 0
    clientpro = 0
    client_docs = 0
    us_process = ''
    eu_process = ''
    not_process = list()
    failure = False
    error = ''
    contents = ''
    pre_content = ''
    send_from = "velkumar.loganathan@datasite.com"
    send_to = ["b4g2z8c3c8w2t1q7@ds1global.slack.com"]
    #b4g2z8c3c8w2t1q7@ds1global.slack.com
    subject = 'Document Reprocessing via Swagger'
    zero_Docs = False
    logging.info('Today' + folder)
    logging.info('Doc Process started')
main()

#
#  Based on the Document Count Email is triggered (Processed, Zero Docs, Not Processed)
#
logging.info('Checking for the Zero_Docs')

if not zero_Docs:
    if len(not_process) == 0:
        logging.info('not processing is zero')
        body = "Hello Team," + "\n" + "\n" + "Docs Reprocessed Today  " + now.strftime(
            '%d/%m/%Y') + "\n" + "\n" + "Time Frame: " + from_date + " to " + to_date + "\n" + "\n" + us_process + "\n" + "\n" + eu_process + "\n" + "\n" + "Kind Regards," + "\n" + "Velkumar"
        send_mail(send_from, send_to, subject, body, file_list)

    elif len(not_process) > 0:
        logging.info('not process files are present')
        body = "Hello Team," + "\n" + "Docs Reprocessed Today  " + now.strftime(
            '%d/%m/%Y') + "\n" + "\n" + "Time Frame: " + from_date + " to " + to_date + "\n" + "\n" + us_process + "\n" + "\n" + eu_process + "\n" + "Please find the Not processed Documents" + "\n" + "Not Processed Docs: " + str(
            len(not_process)) + "\n" + "\n".join(
            map(str, not_process)) + "\n" + "\n" + "Kind Regards," + "\n" + "Velkumar"
        send_mail(send_from, send_to, subject, body, file_list)
else:
    logging.critical('TOW DAYS WE GOT ZERO RECORD.')
    hsend_from = "velkumar.loganathan@datasite.com"
    hsend_to = ["velkumar.loganathan@datasite.com"]
    hsubject = "ZERO RECORD..! Reprocess"
    hbody = "Hello Team," + "\n" + "\n" + "Docs Reprocessed Today  " + now.strftime(
        '%d/%m/%Y') + "\n" + 'TOW DAYS WE GOT ZERO RECORD.' + "\n" + "\n" + 'Please have a look into it' + "\n" + "\n" + "Kind Regards," + "\n" + "Velkumar"
    send_mail(hsend_from, hsend_to, hsubject, hbody, file_list)