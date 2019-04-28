"""
@author: Wang Ying, Ke Min Xi, Wu Xi， Qian RuiYuan

Description
  Functions for preporcessing meme images
"""

import urllib,urllib.request,io,os,pytesseract,time,base64,requests,datetime,imghdr,numpy as np
from bs4 import BeautifulSoup
try:
    from PIL import Image
except ImportError:
    import Image
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.types
#pip3 install https://github.com/OlafenwaMoses/ImageAI/releases/download/2.0.1/imageai-2.0.1-py3-none-any.whl 
#https://github.com/OlafenwaMoses/ImageAI/releases/download/1.0/resnet50_coco_best_v2.0.1.h5
from imageai.Detection import ObjectDetection
#pip install googletrans
from googletrans import Translator
#import pillowfight


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
        imgs = soup.findAll("img",{"src":True})
        print("Starting processing: "+ targetURL)
        print(imgs[0])
        
        #Check image one by one can append the valid one to local path
        for img in imgs: 
            try:
                #JPG, PNG ,GIF, http in src or in data-original and save to local file 
                if ("me.me" in targetURL) and (("jpg" in img["src"]) or ("png" in img["src"]) or ("gif" in img["src"])) and ("http" in img["src"]):
                    Common.saveImageToLocal(self,img["src"], localImagePath)
                    #wait to avoid web blocking
                    Common.wait_while(self,2,1)
                elif ("doutu" in targetURL) and (("jpg" in img["data-original"]) or ("png" in img["data-original"]) or ("gif" in img["data-original"])) and ("http" in img["data-original"]):
                    Common.saveImageToLocal(self,img["data-original"], localImagePath)
                    #wait to avoid web blocking
                    Common.wait_while(self,2,1)
                elif ("jandan" in targetURL) and (("jpg" in img["src"]) or ("png" in img["src"])) and ("w" in img["src"]):
                    Common.saveImageToLocal(self,img["src"], localImagePath)
                    #wait to avoid web blocking
                    Common.wait_while(self,2,1)
                elif ("jandan" in targetURL) and ("gif" in img["org_src"]) and ("w" in img["org_src"]):
                    Common.saveImageToLocal(self,img["org_src"], localImagePath)
                    #wait to avoid web blocking
                    Common.wait_while(self,2,1)
                elif (("jpg" in img["src"]) or ("png" in img["src"]) or ("gif" in img["src"])) and ("http" in img["src"]):
                    Common.saveImageToLocal(self,img["src"], localImagePath)
                    #wait to avoid web blocking
                    Common.wait_while(self,2,1)
            except Exception as e:
                print("processWebImageToLocal ex: " + str(e))

                    
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
            print("File already exists, save failed")
            return 
        
        #save image to local file 
        with open(path, 'wb') as f:
            try:
                if "https" not in imgURL:
                    imgURL = "https:" + imgURL
                f.write(requests.get(imgURL).content)
                
            except Exception as e:
                print("saveImageToLocal ex: " + str(e))
            
    """
      read the text contains in image
    """        
    def readImageText(self,imagePath, language = "all"):
        image = Image.open(imagePath)
        image = image.convert("RGB")
        image = pillowfight.swt(image,output_type=pillowfight.SWT_OUTPUT_ORIGINAL_BOXES)
        return self.extractText(image,language)
    
    """
       read the text contains in image (Base64 format)
    """
    def readImageBase64Text(self, imageBase64,language = "all"):
        image = self.Base64ToImage(imageBase64) 
        image = image.convert("RGB")
#        image = pillowfight.swt(image,output_type=pillowfight.SWT_OUTPUT_ORIGINAL_BOXES)
        return self.extractText(image,language)
    
    """
       Extract image innertext
    """
    def extractText(self, image, language = "all"):
        text = ""
        try:
            if language == "all":
                text = pytesseract.image_to_string(image,lang='eng')
                text += pytesseract.image_to_string(image,lang='chi_sim')
                text += pytesseract.image_to_string(image,lang='chi_tra')
            else:
                text = pytesseract.image_to_string(image,lang=language)
        except Exception as e:
            print("extractText ex" + ": " + str(e))
        return text.replace('\n', '').replace(' ', '').replace('|','')
    
    """
      detect the object in image
      Note:resnet50_coco_best_v... need put under image folder
    """
    def readImageObject(self,imagePath,detector):
        outputstr=""
        
        #detect object
        detections = detector.detectObjectsFromImage(input_image=imagePath)#, output_image_path=os.path.join(execution_path , "image3new.jpg")

        #combine the found out
        for eachObject in detections:
            outputstr += eachObject["name"] + " "
            
        #add the translation   
        outputstr += self.translation(outputstr)
        return outputstr
    
    """
        input image base64 and output detected object
    """
    def readImageBase64Object(self,imageBase64,detector):
        outputstr=""
        img = base64.decodestring(imageBase64)
        fileBytes = io.BytesIO(img)
        image = Image.open(fileBytes)
        image = image.convert("RGB")
        img_a = np.array(image, dtype="float32")
        #detect object
        detections = detector.detectObjectsFromImage(input_image=img_a,input_type= "array")#, output_image_path=os.path.join(execution_path , "image3new.jpg")

        #combine the found out
        for eachObject in detections:
            outputstr += eachObject["name"] + " "
            
        #add the translation   
            outputstr += self.translation(outputstr)
        return outputstr
    
    """
        translation Eng and Chinese Simple
    """
    def translation(self, inputstr):
        translator = Translator()
        if "lang=en" in str(translator.detect(inputstr)):
            return translator.translate(inputstr,dest="zh-CN").text
        elif "lang=zh-CN" in str(translator.detect(inputstr)):
            return translator.translate(inputstr,dest="en").text
    
    """
     Save alll images in target file
     Not the object detection set to false
    """
    def ProcessImageFolderToTextFile(self, imageFolderPath, savePath, isObjDetect = False):
        innertexts = []
        #read all images in image folder
        fileList = os.listdir(imageFolderPath)
        
        detector = ObjectDetection()
        if isObjDetect:        
            #detect the object and obtain a list            
            detector.setModelTypeAsRetinaNet()
            detector.setModelPath(os.path.join(imageFolderPath, "resnet50_coco_best_v2.0.1.h5"))
            detector.loadModel()
        i=1
        
        #read image one by one and save the innertext in the list
        for file in fileList:
            print("Processing ",str(i)," image: ",file)
            i+=1
            if not (".png" in file.lower() or ".jpg" in file.lower() or ".gif" in file.lower()):
                print("Image type wrong:",file)
            else:
                fullfilePath = os.path.abspath(imageFolderPath.split("\\")[-2]+"\\"+file)
                imagebase64 = base64.b64encode(open(fullfilePath,'rb').read())
            
                #Read the image - inner text and object detection
                try:
                    #read inner text
                    innerTag = (self.readImageBase64Text(imagebase64,"eng").replace('\n', '').replace(' ', '').replace('|','')) 
                    #read object detection
                    if isObjDetect:
                        innerTag += (self.readImageBase64Object(imagebase64,detector).replace('\n', '').replace(' ', '').replace('|',''))
                except Exception as e:
                    print("ProcessImageFolderToTextFile ex " + fullfilePath + ": " + str(e))
                #if no innertext found, write no tag
                innerTag = ("NoTag") if innerTag == "" else (innerTag.lower())
            
                #image in base64 formate
                imgType = imghdr.what(fullfilePath)
                imagebase64 = base64.b64encode(open(fullfilePath,'rb').read())
            
                #add to list
                innertext = file+"|"+ innerTag + "|data:image/" +imgType+";base64," + str(imagebase64, 'utf-8')
                innertexts.append(innertext)

        
        #write the information into the target file
        with open(savePath + datetime.datetime.now().strftime("%m%d%Y%H%M%S%f")+".txt", 'a',encoding='utf-8') as the_file: 
            the_file.writelines("Image|Tag|Base64\n")
            for txt in innertexts:
                the_file.writelines(txt.strip() + '\n')
                
    """
        transfer image folder to text folder
    """
    def transferImageFolderToTXT(self, imageFolderPath, savePath, saveFileName = ""):
        innertexts = []
        #read all images in image folder
        fileList = os.listdir(imageFolderPath)
        i = 1
        #read image one by one and save the innertext in the list
        for file in fileList:
            try:
                print("Processing ",str(i)," image: ",file)
                i += 1
                #only process gif jpg and png file
                if not (".png" in file.lower() or ".jpg" in file.lower() or ".gif" in file.lower()):
                    print("Image type wrong:",file)
                else:
                
                    fullfilePath = os.path.abspath(imageFolderPath.split("\\")[-2]+"\\"+file)
                    imagebase64 = base64.b64encode(open(fullfilePath,'rb').read())
                    
                    #image in base64 formate
                    imgType = imghdr.what(fullfilePath)
                    imagebase64 = base64.b64encode(open(fullfilePath,'rb').read())
                    
                    #add to list
                    innertext = file + "|data:image/" +imgType+";base64," + str(imagebase64, 'utf-8')
                    innertexts.append(innertext)
            except Exception as e:
                print("transferImageFolderToTXT ex: " + str(e))
        
        if saveFileName == "":
            saveFileName = datetime.datetime.now().strftime("%m%d%Y%H%M%S%f")+".txt"
        #write the information into the target file
        with open(savePath + saveFileName, 'a',encoding='utf-8') as the_file: 
#            the_file.writelines("Image|Base64\n")
            for txt in innertexts:
                the_file.writelines(txt.strip() + '\n')
    
    """
     Use spark sql to read the text and filter the search information
    """
    def sparkSQLReadNFilterList(self, textFilePath, searchStr):
        #set the search string
        searchList = list(str(searchStr).lower().replace(" ",""))
        searchStr = "%".join(searchList)
        searchStr = "%"+str(searchStr).lower()+"%"
        
        #set the spark context and spark sql
        sc = SparkContext.getOrCreate()
        sqlContext = SQLContext(sc)
        
        #read the text file
        df=sqlContext.read.option("header","true").option("delimiter","|").option("inferSchema","true")\
            .schema(
                    pyspark.sql.types.StructType(
                            [
                                    pyspark.sql.types.StructField("path",pyspark.sql.types.StringType()),
                                    pyspark.sql.types.StructField("features",pyspark.sql.types.StringType()),
                                    pyspark.sql.types.StructField("binary",pyspark.sql.types.StringType())
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
    
    def sprkSQLReadDFtoList(self, dfPath, searchStr):
#        finalDatasetPath = "hdfs:///dataFrames/final8"
        #set the search string
        
        searchList = list(str(searchStr).lower().replace(" ",""))
        searchStr = "%".join(searchList)
        searchStr = "%"+str(searchStr).lower()+"%"
        
        #set the spark context and spark sql
        conf = SparkConf()#.setAppName("Upload Images to HDFS").setMaster("yarn")
        #sc = SparkContext(conf=conf)
        sc = SparkContext.getOrCreate(conf=conf)
        sqlContext = SQLContext(sc)
        
        df = sqlContext.read.parquet(dfPath)

        #filter the data with the search string           
        df = df.filter(df["features"].like(searchStr))
        
        #only output the image base64
        dflist = df.select('binary').collect()
        
        #change the format to row
        dfreturn = [str(row.binary) for row in dflist]
        return dfreturn

   
    
    def sparkSQLIsRepeat(self, textFilePath, imageBase64):
        #set the spark context and spark sql
        imageBase64 = "%" + imageBase64 + "%"
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
        df = df.filter(df["Base64"].like(imageBase64))
        
        #check if same image contained
        dfsize = df.count()
        
        #change the format to row
        if dfsize == 0:
            return True
        else:
            return False
    
    """
      convert image to base64 format
    """
    def imageToBase64(self,imagePath):
        return base64.b64encode(open(imagePath,'rb').read())
    
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
#            print(max_time)
            time.sleep(delta)
            return False
    
    """
       Rename all image files
    """
    def renameFiles(self, imageFolderPath):
        fileList = os.listdir(imageFolderPath)
        intTime = int(datetime.datetime.now().strftime("%Y%m%d%H%M%S%f"))
        for file in fileList:
            
            if (".jpg" in file or ".png" in file or ".gif" in file):
                old_file = os.path.join(imageFolderPath, file)
                newFileName = str(intTime) + "." + file.split(".")[-1]
                new_file = os.path.join(imageFolderPath, newFileName)
                os.rename(old_file, new_file)
                intTime -=1
