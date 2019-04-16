# -*- coding: utf-8 -*-

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


# ======== Routing =========================================================== #
# -------- Login ------------------------------------------------------------- #
@app.route('/', methods=['GET', 'POST'])
def login():
    if not session.get('logged_in'):
        form = forms.LoginForm(request.form)
        if request.method == 'POST':
            username = request.form['username'].lower()
            password = request.form['password']
            if form.validate():
                #if helpers.credentials_valid(username, password):
                    session['logged_in'] = True
                    session['username'] = username
                    return json.dumps({'status': 'Login successful'})
                #return json.dumps({'status': 'Invalid user/pass'})
            return json.dumps({'status': 'Both fields required'})
        return render_template('login.html', form=form)
    user = helpers.get_user()
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
            simages = commonF.sparkSQLReadNFilterList(imagePath,sskey)
            
            sskey = "You are searching " + sskey
    return render_template('home.html', user=user,searchkey=sskey,hists=simages)


@app.route("/logout")
def logout():
    session['logged_in'] = False
    return redirect(url_for('login'))


# -------- Signup ---------------------------------------------------------- #
@app.route('/signup', methods=['GET', 'POST'])
def signup():
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
    if session.get('logged_in'):
        if request.method == 'POST':
            keywords = request.form['keywords']
            session['keyword'] = keywords
            #filePath = "C:\\Users\\candy\\Documents\\HKU\\Semester2\\COMP7305\\Group Project\\Test code-save and read binary image\\"
            #filePath = "/home/hduser/MemeRepo_CloudComputing-/Flaskex"
            #filePath = "hdfs://gpu3:9000/wuxi/04122019203919021146.txt"
            filePath = "04122019203919021146.txt"
            session['imagePath'] = filePath
            keywords= keywords.encode("utf-8").decode("latin1")
            print (keywords)
            return json.dumps({'status': 'Search successful'})
        #user = helpers.get_user()
        return render_template('home.html')
    return redirect(url_for('login'))
basedir = os.path.abspath(os.path.dirname(__file__)) 

ALLOWED_EXTENSIONS = set(['png', 'jpg', 'JPG', 'PNG', 'bmp','gif'])
 
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS

@app.route('/up_photo', methods=['post'])
def up_photo():
    img = request.files.get('photo')
    #username = request.form.get("name")
    if (not img):
        return redirect('/')
    if not (img and allowed_file(img.filename)):
        return jsonify({"error": 1001, "msg": "Only support .png .PNG .jpg .JPG .bmp .gif"})
    path = basedir+"/static/photo/"
    file_path = path+img.filename
    img.save(file_path)
    '''
    encoded_string=""
    with open(file_path, "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read())
    #print (encoded_string)
    '''
    imgType = imghdr.what(file_path)
    imagebase64 = base64.b64encode(open(file_path,'rb').read())
    commonF = Common()
    if (not commonF.sparkSQLIsRepeat('04122019203919021146.txt',str(imagebase64, 'utf-8'))):
        return redirect('/') 
    x=commonF.readImageText(file_path,"all")
    x=re.sub('\s','',x)
    x=x.replace('\n', '').replace(' ', '').replace('|','')
    x=("NoTag") if x == "" else (x.lower())
    sstring = img.filename +"|"+ x + "|data:image/" +imgType+";base64," + str(imagebase64, 'utf-8')
    nowstring=sstring.encode("utf-8").decode("latin1")
    print (nowstring)
    file = '04122019203919021146.txt'
    with open(file, 'a+') as f:
        f.write(nowstring)
        f.write('\n')
        f.close()
 #   with hdfs.open('hdfs://gpu3:9000/wuxi/04122019203919021146.txt', 'a') as f:
 #       f.write(nowstring)
    #return render_template('home.html')
    return redirect('/')
# ======== Main ============================================================== #
if __name__ == "__main__":
    app.secret_key = os.urandom(12)  # Generic key for dev purposes only
    app.run(host='0.0.0.0',debug=True, use_reloader=True)
