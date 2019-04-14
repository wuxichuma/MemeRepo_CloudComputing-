"""
@author: Wang Ying, Ke Min Xi, Wu Xi

Description
  Functions for preporcessing meme images
"""

import urllib,urllib.request,io,os,pytesseract,time,base64,requests,datetime,imghdr
from bs4 import BeautifulSoup
try:
    from PIL import Image
except ImportError:
    import Image
from pyspark import SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.types
#pip3 install https://github.com/OlafenwaMoses/ImageAI/releases/download/2.0.1/imageai-2.0.1-py3-none-any.whl 
#https://github.com/OlafenwaMoses/ImageAI/releases/download/1.0/resnet50_coco_best_v2.0.1.h5
#For linux, if cannot find cv2
#pip3 install opencv-python

from imageai.Detection import ObjectDetection
#pip install googletrans
from googletrans import Translator

class Common():
    """
    Search images on the web page and download to local
    """    
    def processWebImageToLocal(self,targetURL, localImagePath):
        #Add an extra header, since some website stop the web downloading
        user_agent = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'
        headers={'User-Agent':user_agent,} 
        
        #Request the URL for obtain the HTML information
        req=urllib.request.Request(targetURL,None,headers)
        thepage = urllib.request.urlopen(req)#urllib.request.urlopen(targetURL,headers=hdr)
        soup = BeautifulSoup(thepage)
        
        #Filter the webpage and obtain all image which have src and class
        imgs = soup.findAll("img",{"src":True, "class":True})
        #print(imgs[0])
        
        #Check image one by one can append the valid one to local path
        for img in imgs: 
            try:
                #wait to avoid web blocking
                Common.wait_while(self,3,1)
                
                #JPG, PNG ,GIF, http in src or in data-original and save to local file 
                if ("me.me" in targetURL) and (("jpg" in img["src"]) or ("png" in img["src"]) or ("gif" in img["src"])) and ("http" in img["src"]):
                    Common.saveImageToLocal(self,img["src"], localImagePath)
                elif ("doutu" in targetURL) and (("jpg" in img["data-original"]) or ("png" in img["data-original"]) or ("gif" in img["data-original"])) and ("http" in img["data-original"]):
                    Common.saveImageToLocal(self,img["data-original"], localImagePath)
                elif ("jiandan" in targetURL) and (("jpg" in img["src"]) or ("png" in img["src"])) and ("w" in img["src"]):
                    Common.saveImageToLocal(self,img["src"], localImagePath)
                elif ("jiandan" in targetURL) and ("gif" in img["org_src"]) and ("w" in img["org_src"]):
                    Common.saveImageToLocal(self,img["org_src"], localImagePath)
                elif (("jpg" in img["src"]) or ("png" in img["src"]) or ("gif" in img["src"])) and ("http" in img["src"]):
                    Common.saveImageToLocal(self,img["src"], localImagePath)
            except:
                print(img)

                    
    """
     Save image to local path
    """        
    def saveImageToLocal(self,imgURL,rootPath):
        #define the file name yyyyMMddHHmmssff
        fileName = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
        #define the path of the saved file and format
        path = rootPath+fileName+"."+imgURL.split(".")[-1]
        
        #check if file is exists
        exists = os.path.isfile(path)
        if exists:
            print("*********************")
            return 
        
        #save image to local file 
        with open(path, 'wb') as f:
            try:
                if "https" not in imgURL:
                    imgURL = "https:" + imgURL
                f.write(requests.get(imgURL).content)
                
            except:
                print("exception")
            
    """
      read the text contains in image
    """        
    def readImageText(self,imagePath):
        image = Image.open(imagePath)
        image = image.convert("RGB")
        return pytesseract.image_to_string(image,lang='chi_sim+eng')
    
    """
      detect the object in image
      Note:resnet50_coco_best_v... need put under image folder
    """
    def readImageObject(self,imagePath):
        outputstr=""
        translator = Translator()
        execution_path = os.path.dirname(imagePath)
        
        #detect the object and obtain a list
        detector = ObjectDetection()
        detector.setModelTypeAsRetinaNet()
        detector.setModelPath(os.path.join(execution_path , "resnet50_coco_best_v2.0.1.h5"))
        detector.loadModel()
        detections = detector.detectObjectsFromImage(input_image=imagePath)#, output_image_path=os.path.join(execution_path , "image3new.jpg")

        #combine the found out
        for eachObject in detections:
            outputstr += eachObject["name"] + " "
            
        #add the translation   
        if "lang=en" in str(translator.detect(outputstr)):
            outputstr += translator.translate(outputstr,dest="zh-CN").text
        elif "lang=zh-CN" in str(translator.detect(outputstr)):
            outputstr += translator.translate(outputstr,dest="en").text
        return outputstr
    
    """
     Save alll images in target file
     Not the object detection set to false
    """
    def ProcessImageFolderToTextFile(self, imageFolderPath, savePath, isObjDetect = False):
        innertexts = []
        #read all images in image folder
        fileList = os.listdir(imageFolderPath)
        i=1
        
        #read image one by one and save the innertext in the list
        for file in fileList:
            print("Processing ",str(i)," image: ",file)
            fullfilePath = os.path.abspath(imageFolderPath.split("\\")[-2]+"\\"+file)
            
            #Read the image - inner text and object detection
            try:
                #read inner text
                innerTag = (self.readImageText(fullfilePath).replace('\n', '').replace(' ', '').replace('|','')) 
                #read object detection
                if isObjDetect:
                    innerTag += (self.readImageObject(fullfilePath).replace('\n', '').replace(' ', '').replace('|',''))
            except Exception as e:
                print("ex" + fullfilePath + ":" + str(e))
            #if no innertext found, write no tag
            innerTag = ("NoTag") if innerTag == "" else (innerTag.lower())
            
            #image in base64 formate
            imgType = imghdr.what(fullfilePath)
            imagebase64 = base64.b64encode(open(fullfilePath,'rb').read())
            
            #add to list
            innertext = file+"|"+ innerTag + "|data:image/" +imgType+";base64," + str(imagebase64, 'utf-8')
            innertexts.append(innertext)
            i+=1
#            if i == 10:
#                break
        
        #write the information into the target file
        with open(savePath + datetime.datetime.now().strftime("%m%d%Y%H%M%S%f")+".txt", 'a',encoding='utf-8') as the_file: 
            the_file.writelines("Image|Tag|Base64\n")
            for txt in innertexts:
                the_file.writelines(txt.strip() + '\n')
    
    """
     Use spark sql to read the text and filter the search information
    """
    def sparkSQLReadNFilterList(self, textFilePath, searchStr):
        #set the search string
        searchStr = "%"+str(searchStr).lower()+"%"
        
        #set the spark context and spark sql
        sc = SparkContext.getOrCreate()
        sqlContext = SQLContext(sc)
        
        #read the text file
        df=sqlContext.read.option("header","true").option("delimiter","|").option("inferSchema","true")\
            .schema(
                    pyspark.sql.types.StructType(
                            [
                                    pyspark.sql.types.StructField("Image",pyspark.sql.types.StringType()),
                                    pyspark.sql.types.StructField("Tag",pyspark.sql.types.StringType()),
                                    pyspark.sql.types.StructField("Base64",pyspark.sql.types.StringType())
                                    ]
                            )
                    )\
            .csv(textFilePath)
        
        #filter the data with the search string           
        df = df.filter(df["Tag"].like(searchStr))
        
        #only output the image base64
        dflist = df.select('Base64').collect()
        
        #change the format to row
        dfreturn = [str(row.Base64) for row in dflist]
        return dfreturn
    
    """
      convert image to base64 format
    """
    def imageToBase64(self,image):
        return base64.b64encode(image)
    
    """
      convert base64 string to image format
    """
    def Base64ToImage(self,b):
        img = base64.decodestring(b)
        fileBytes = io.BytesIO(img)
        return Image.open(fileBytes)
    
    """
      single image url to base64
    """
    def urlToBase64(self,url):
        response = requests.get(url, timeout=5)   
        return base64.b64encode(response.content)
    
    """
      let code hanging for a while
    """
    def wait_while(self,timeout, delta=1):
        """
        @condition: lambda function which checks if the text contains "REALTIME"
        @timeout: Max waiting time
        @delta: time after which another check has to be made
        """
        max_time = time.time() + timeout
        while max_time > time.time():
            print(max_time)
            time.sleep(delta)
            return False
        