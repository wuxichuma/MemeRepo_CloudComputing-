# -*- coding: utf-8 -*-
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
from scripts import tabledef
from scripts import forms
from scripts import helpers
from flask import jsonify,Flask, redirect, url_for, render_template, request, session
import json
import imghdr
import base64
#from pydoop import hdfs
import re
import sys
import os
from Common import Common


app = Flask(__name__)

dataFrameUrl = "hdfs://gpu3:9000/dataFrames/final9"


# ======== Routing =========================================================== #
# -------- Login ------------------------------------------------------------- #
@app.route('/', methods=['GET', 'POST'])
def login():

    '''
    Login check
    '''
    if not session.get('logged_in'):
        form = forms.LoginForm(request.form)
        if request.method == 'POST':
            username = request.form['username'].lower()
            password = request.form['password']
            if form.validate():
                if helpers.credentials_valid(username, password):
                    session['logged_in'] = True
                    session['username'] = username
                    return json.dumps({'status': 'Login successful'})
                return json.dumps({'status': 'Invalid user/pass'})
            return json.dumps({'status': 'Both fields required'})
        return render_template('login.html', form=form)
    user = helpers.get_user()

    '''
    Return memems from HDFS

    Common.py is our core libray written by ourselves to use SparkSQL or do inner-text dection 
    Common.py can be seen in .../Flaskex/
    '''

    sskey=""
    if 'keyword' in session:
        sskey = session["keyword"]
    simages =""
    if 'imagePath' in session:
        if session['imagePath'] != "":
            imagePath = session['imagePath']
            print(imagePath)
            if (sskey==""):
                return render_template("home.html",user=user)
            commonF = Common()
            # Common is written by ourselves to run SparkSQL
            simages = commonF.sprkSQLReadDFtoList(imagePath,sskey)
            # sprkSQLReadDFtoList is to get images from hdfs using SparkSQL
            sskey = "You are searching " + sskey
    return render_template('home.html', user=user,searchkey=sskey,hists=simages)


@app.route("/logout")
def logout():
    session['logged_in'] = False
    return redirect(url_for('login'))


# -------- Signup ---------------------------------------------------------- #
@app.route('/signup', methods=['GET', 'POST'])
def signup():

    '''
    For new user to sign up, nothing interesting
    '''

    if not session.get('logged_in'):
        form = forms.LoginForm(request.form)
        if request.method == 'POST':
            username = request.form['username'].lower()
            password = helpers.hash_password(request.form['password'])
            email = request.form['email']
            if form.validate():
                if not helpers.username_taken(username):
                    helpers.add_user(username, password, email)
                    session['logged_in'] = True
                    session['username'] = username
                    session['keyword'] = ""
                    return json.dumps({'status': 'Signup successful'})
                return json.dumps({'status': 'Username taken'})
            return json.dumps({'status': 'User/Pass required'})
        return render_template('login.html', form=form)
    return redirect(url_for('login'))


# -------- Settings ---------------------------------------------------------- #
@app.route('/settings', methods=['GET', 'POST'])
def settings():

    '''
    Nothing interesting
    '''

    if session.get('logged_in'):
        if request.method == 'POST':
            password = request.form['password']
            if password != "":
                password = helpers.hash_password(password)
            email = request.form['email']
            helpers.change_user(password=password, email=email)
            return json.dumps({'status': 'Saved'})
        user = helpers.get_user()
        return render_template('settings.html', user=user)
    return redirect(url_for('login'))


    
# -------- search ---------------------------------------------------------- #
@app.route('/home', methods=['GET', 'POST'])
def home():
    '''
    Get keywords from webpage and save it into session, then redirect to other page to do searching
    '''
    if session.get('logged_in'):
        if request.method == 'POST':
            keywords = request.form['keywords']
            session['keyword'] = keywords
            filePath= dataFrameUrl       #hdfs://gpu3:9000/dataFrames/final9"                                       
            session['imagePath'] = filePath
            keywords= keywords.encode("utf-8").decode("latin1")
            print (keywords)
            return json.dumps({'status': 'Search successful'})
        #user = helpers.get_user()
        return render_template('home.html')
    return redirect(url_for('login'))



basedir = os.path.abspath(os.path.dirname(__file__)) 

ALLOWED_EXTENSIONS = set(['png', 'jpg', 'JPG', 'PNG', 'bmp','gif','jpeg','JPEG'])
 
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS

@app.route('/up_photo', methods=['post'])
def up_photo():

    '''
    For uploading photo into HDFS

    '''


    img = request.files.get('photo')
    #Get photo from webpage

    if (not img):
        return redirect('/')
    if not (img and allowed_file(img.filename)):
        return jsonify({"error": 1001, "msg": "Only support .png .PNG .jpg .JPG .bmp .gif"})

    '''
    If not supported images, return error
    
    ''' 

    path = basedir+"/static/photo/"
    imgfilename=img.filename.encode("utf-8").decode("latin1")
    file_path = path+imgfilename
    img.save(file_path)


    '''
    Save the image in local directory 
    '''

    imgType = imghdr.what(file_path)
    imagebase64 = base64.b64encode(open(file_path,'rb').read())
    commonF = Common()
    x=commonF.readImageText(file_path,"all")

    '''
    Get type, inner-text and base64 of that image 

    '''



    x=re.sub('\s','',x)
    x=x.replace('\n', '').replace(' ', '').replace('|','')
    x=("NoTag") if x == "" else (x.lower())
    sstring = img.filename +"|"+ x + "|data:image/" +imgType+";base64," + str(imagebase64, 'utf-8')
    nowstring=sstring.encode("utf-8").decode("latin1")
    
    conf = SparkConf()#.setAppName("Upload One Image to HDFS").setMaster("yarn")
    #sc = SparkContext(conf=conf)
    sc = SparkContext.getOrCreate(conf=conf)
    sqlContext = SQLContext(sc)
    
    uploadedDF = sc.parallelize( [ (img.filename,x,"data:image/" +imgType+";base64," + str(imagebase64, 'utf-8')) ]).toDF(["path","features","binary"])
    uploadedDF.write.mode('append').parquet(dataFrameUrl)  #("hdfs://gpu3:9000/dataFrames/final9")

    '''
    Save it into HDFS 
    '''
    print (nowstring)
    return redirect('/')


# ======== Main ============================================================== #
if __name__ == "__main__":
    app.secret_key = os.urandom(12)  # Generic key for dev purposes only
    app.run(host='0.0.0.0',debug=True, use_reloader=True)
