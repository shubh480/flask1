from flask import Flask, jsonify, request
import json
import requests
import re
import base64
import pymysql
import boto3
from config import connection_properties,awskey

app = Flask(__name__)
connection_properties = {
    'host': 'moviebuff.cmayuty1ln5o.ap-south-1.rds.amazonaws.com',
    'port': 3306,
    'user': 'admin',
    'passwd': 'moviebuffdb',
    'db':'movietmdb'
    }

access_key = {
    'aws_access_key_id':'AKIA2LMVPTSKSR5ATP7H',
    'aws_secret_access_key':'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'
}
#Screen 1 API to for login
@app.route('/login',methods=['POST'])
def login():
    connection=pymysql.connect(**connection_properties)
    content=request.get_json()  
    screenname=content["ScreenName"]
    fbid=content["FacebookId"]
    userid=content["UserId"]
    password=content["Password"]
    userid=str(userid)
    
    try:
        cur=connection.cursor()
        """
        try:
            query="INSERT INTO usert(sreen_name,country) VALUES (\'"+screenname+"\',\'"+country+"\');"
            cur.execute(query)
            
        except:
            return "Screenname already exist!"
        cur.execute("commit")
        """
        password_check=""
        if len(userid)==0 and len(fbid)==0:
            cur.execute("SELECT u_password from usert where screen_name=\""+screenname+"\";")
            pass_result=cur.fetchone()
            row_headers=[x[0] for x in cur.description]
            pass_result=dict(zip(row_headers,pass_result))
            password_check=pass_result['u_password']
            cur.execute("SELECT usert.screen_name as \"Name\", usert.user_id as \"Id\", usert.img_url as \"ImgUrl\",achievements.u_coins as \"Coins\",achievements.u_level as \"Level\",achievements.correct_images as \"CorrectAnswers\",achievements.u_crowns as \"Crowns\",achievements.gold as \"Gold\",achievements.silver as \"Silver\",achievements.bronze as \"Bronze\",usert.wtg as \"WhereToGo\" FROM movietmdb.usert INNER JOIN movietmdb.achievements ON usert.user_id=achievements.user_id and usert.screen_name=\""+screenname+"\";")
        elif len(screenname)==0 and len(fbid)==0:
            cur.execute("SELECT u_password from usert where user_id=\""+userid+"\";")
            pass_result=cur.fetchone()
            row_headers=[x[0] for x in cur.description]
            pass_result=dict(zip(row_headers,pass_result))
            password_check=pass_result['u_password']
            cur.execute("SELECT usert.screen_name as \"Name\", usert.user_id as \"Id\", usert.img_url as \"ImgUrl\",achievements.u_coins as \"Coins\",achievements.u_level as \"Level\",achievements.correct_images as \"CorrectAnswers\",achievements.u_crowns as \"Crowns\",achievements.gold as \"Gold\",achievements.silver as \"Silver\",achievements.bronze as \"Bronze\",usert.wtg as \"WhereToGo\" FROM usert INNER JOIN achievements ON usert.user_id=achievements.user_id and usert.user_id=\""+userid+"\";")
        elif len(userid)==0 and len(screenname)==0:
            cur.execute("SELECT usert.screen_name as \"Name\", usert.user_id as \"Id\", usert.img_url as \"ImgUrl\",achievements.u_coins as \"Coins\",achievements.u_level as \"Level\",achievements.correct_images as \"CorrectAnswers\",achievements.u_crowns as \"Crowns\",achievements.gold as \"Gold\",achievements.silver as \"Silver\",achievements.bronze as \"Bronze\",usert.wtg as \"WhereToGo\" FROM usert INNER JOIN achievements ON usert.user_id=achievements.user_id and usert.fb_id=\""+fbid+"\";")
            password_check=""
        if password==password_check:
            row_headers=[x[0] for x in cur.description] #this will extract row headers
            result=cur.fetchall()
            json_data=[]
            for r in result:
                json_data.append(dict(zip(row_headers,r)))
            response={'IsSuccess':True,'error':'null','Data':json_data}
        else:
            response={'IsSuccess':True,'error':'null','Data':[]}
        cur.close()
        connection.close()
        return jsonify(response)
    except Exception as e:
        response={'IsSuccess':False,'error':str(e)}
        cur.close()
        connection.close()
        return jsonify(response)

#Screen 1 API to get screenname
@app.route('/register/<screenname>',methods=['GET'])
def screennameAPI(screenname):
    try:
        connection=pymysql.connect(**connection_properties)
        cur=connection.cursor()
        cur.execute("select exists(select screen_name from usert where screen_name=\""+screenname+"\");")
        result=cur.fetchone()
        result=int(str(result[0]))
        e_result={
                'Available':''
        }
        if result:
            e_result['Available']=False 
        else:
            e_result['Available']=True
        response={'IsSuccess':True,'error':'null','Data':[]}
        response['Data']=e_result
        cur.close()
        connection.close()
        return jsonify(response)
    except Exception as e:
        response={'IsSuccess':False,'error':str(e)}
        cur.close()
        connection.close()
        return jsonify(response)

#Screen 2 API avatar retrieval 
@app.route('/avatar',methods=['GET'])
def avatarAPI():
    try:
        connection=pymysql.connect(**connection_properties)
        cur=connection.cursor()
        cur.execute("select * from avatar")
        row_headers=[x[0] for x in cur.description] #this will extract row headers
        result=cur.fetchall()
        json_data=[]
        response={'IsSuccess':True,'error':'null','Data':[]}
        for result in result:
            json_data.append(dict(zip(row_headers,result)))
        response['Data']=json_data
        cur.close()
        connection.close()
        return jsonify(response)
    except Exception as e:
        response={'IsSuccess':False,'error':str(e)}
        cur.close()
        connection.close()
        return jsonify(response)


#Screen 3 API user profile
@app.route('/profile',methods=['POST'])
def ProfileAPI():
    connection=pymysql.connect(**connection_properties)
    cur=connection.cursor()
    content=request.get_json()
    country=content["Country"]
    fbid=content["FB Id"]
    screenname=content["ScreenName"]
    fname=content["FirstName"]
    lname=content["LastName"]
    mstatus=content["MaritalStatus"]
    if mstatus=="True" or mstatus=="true":
        mstatus=1
    elif mstatus=="false" or mstatus=="False":
        mstatus=0
    prof=content["Profession"]
    email=content["Email Id"]
    aid=content["AvatarId"]
    abase64=content["AvatarBase64"]
    afb=content["AvatarFacebook"]
    sgenre=content["SelectedGenre"]
    sregion=content["SelectedRegion"]
    password=content["Password"]
    try:
        if abase64:
            with open("/tmp/output"+screenname+".jpg", "wb") as fh:
                fh.write(base64.b64decode(abase64))
            s3_client=boto3.client('s3',**awskey)
            s3_client.upload_file("/tmp/output"+screenname+".jpg","movie.buff.user.images",screenname+".jpg", ExtraArgs={'ACL': 'public-read','ContentType':'image/jpeg'})
            urls="https://s3.ap-south-1.amazonaws.com/movie.buff.user.images/"+screenname+".jpg"
            cur.execute("insert into usert(first_name,last_name,screen_name,u_password,marital_s,prof,gmail_id,img_url,favgenre,region) values (\""+fname+"\",\""+lname+"\",\""+screenname+"\",\""+str(password)+"\",\""+str(mstatus)+"\",\""+prof+"\",\""+email+"\",\""+urls+"\",\""+str(sgenre)+"\",\""+str(sregion)+"\");")
            cur.execute("commit")
        elif afb:
            cur.execute("insert into usert(first_name,last_name,fb_id,screen_name,u_password,marital_s,prof,gmail_id,img_url,favgenre,region) values (\""+fname+"\",\""+lname+"\",\""+fbid+"\",\""+screenname+"\",\""+str(password)+"\",\""+str(mstatus)+"\",\""+prof+"\",\""+email+"\",\""+afb+"\",\""+str(sgenre)+"\",\""+str(sregion)+"\");")
            cur.execute("commit")
        else:
            cur.execute("insert into usert(first_name,last_name,screen_name,u_password,marital_s,prof,gmail_id,avatar_id,favgenre,region) values (\""+fname+"\",\""+lname+"\",\""+screenname+"\",\""+str(password)+"\",\""+str(mstatus)+"\",\""+prof+"\",\""+email+"\",\""+str(aid)+"\",\""+str(sgenre)+"\",\""+str(sregion)+"\");")
            cur.execute("update usert set img_url=(select a_img_url from avatar where avatar_id="+str(aid)+") where usert.screen_name=\""+screenname+"\";")
            cur.execute("commit")
        cur.execute("SELECT user_id from usert WHERE screen_name=\""+screenname+"\";")
        userID=cur.fetchone()
        row_headers=[x[0] for x in cur.description]
        userID=dict(zip(row_headers,userID))
        userid=userID['user_id']
        cur.execute("insert into achievements(user_id) values ("+str(userid)+");")
        cur.execute("COMMIT;")
        cur.execute("Select usert.user_id as \"Id\", achievements.u_coins as \"Coins\", achievements.u_crowns as \"Crowns\" ,achievements.gold as \"Gold\",achievements.silver as \"Silver\",achievements.bronze as \"Bronze\",usert.img_url as \"ImgUrl\",usert.wtg as \"WhereToGo\" from achievements join usert on usert.user_id=achievements.user_id where usert.screen_name=\""+screenname+"\";")
        row_headers=[x[0] for x in cur.description] #this will extract row headers
        myresult=cur.fetchall()
        response={'IsSuccess':True,'error':'null','Data':[]}
        json_data=[]
        for result in myresult:
            json_data.append(dict(zip(row_headers,result)))
        response['Data']=json_data
        cur.close()
        connection.close()
        return jsonify(response)
    except Exception as e:
        response={'IsSuccess':False,'error':str(e)}
        cur.close()
        connection.close()
        return jsonify(response)    

#Screen 8 & 9 Main game API
@app.route('/getQuestions',methods=['POST'])
def startGameAPI():
    connection=pymysql.connect(**connection_properties)
    cur=connection.cursor()
    #read json
    content=request.get_json()
    isr=content["isRandom"]
    region=content["Region"]
    noq=content["noQ"]
    userid=content["user_id"]
    decade=content["Era"]
    userid=str(userid)
    cur.execute("select exists(select user_id from user_questions where user_id="+userid+");")
    check=cur.fetchone()
    c=0
    if 0 in check:
        c=0
    else:
        c=1
    
    language=""
    if region[0]==1:
        language="English"
    elif region[0]==2:
        language="Hindi"
    else:
        return "Enter valid Region"

    #cur.execute("SELECT u_coins FROM achievements WHERE user_id="+userid+";")
    #check=cur.fetchone()
    #check=list(check)
    #usercoin = check[0]
    coins_reduce=int(noq)*2
    #if usercoin>=coins_reduce:
    cur.execute("UPDATE achievements SET u_coins = u_coins - "+str(coins_reduce)+" WHERE user_id="+userid+";")
    cur.execute("COMMIT;")
    """else:
        cur.close()
        connection.close()
        return "Not Enough Coins"  """
    try:
        #Question
        if str(isr) == "True" or str(isr)=="true":
            if c==1:
                if len(region)==2:
                    cur.execute("SELECT image.image_URL as \"ImgUrl\",movie.movie_id as \"Qid\", movie.m_title as \"Answer\" FROM movie JOIN image ON movie.movie_id=image.movie_id join lang on movie.movie_id=lang.movie_id AND movie.movie_id not in (SELECT qid FROM user_questions WHERE user_id="+userid+") GROUP BY movie.movie_id order by rand() limit "+str(noq)+";")
                else:
                    cur.execute("SELECT image.image_URL as \"ImgUrl\",movie.movie_id as \"Qid\", movie.m_title as \"Answer\" FROM movie JOIN image ON movie.movie_id=image.movie_id join lang on movie.movie_id=lang.movie_id where lang.m_lang=\""+language+"\" AND movie.movie_id not in (SELECT qid FROM user_questions WHERE user_id="+userid+") GROUP BY movie.movie_id order by rand() limit "+str(noq)+";")       
            else:
                if len(region)==2:
                    cur.execute("SELECT image.image_URL as \"ImgUrl\",movie.movie_id as \"Qid\", movie.m_title as \"Answer\" FROM movie JOIN image ON movie.movie_id=image.movie_id join lang on movie.movie_id=lang.movie_id AND movie.movie_id not in (SELECT qid FROM user_questions WHERE user_id="+userid+") GROUP BY movie.movie_id order by rand() limit "+str(noq)+";")
                else:
                    cur.execute("SELECT image.image_URL as \"ImgUrl\",movie.movie_id as \"Qid\", movie.m_title as \"Answer\" FROM movie JOIN image ON movie.movie_id=image.movie_id join lang on movie.movie_id=lang.movie_id where lang.m_lang=\""+language+"\" AND movie.movie_id not in (SELECT qid FROM user_questions WHERE user_id="+userid+") GROUP BY movie.movie_id order by rand() limit "+str(noq)+";")       

            row_headers = [x[0] for x in cur.description]  # this will extract row headers
            myresult = cur.fetchall()
            myresult=list(myresult)
            json_data=[]
            cid=[]
            ans=[]
            j=0
            for i in myresult:
                qresult=dict(zip(row_headers,i))
                cid.append(qresult["Qid"])
                ans.append(qresult["Answer"])

            #options
                if len(region)==1:
                    cur.execute("select aoptions.optext as \"options\" from aoptions join lang on lang.movie_id=aoptions.opid join movie on aoptions.opid=movie.movie_id where aoptions.opid!= \""+cid[j]+"\" and lang.m_lang=\""+language+"\" AND movie.m_release>\"1970\" AND movie.m_release<\"2020\" order by rand() limit 3;")
                else:
                    cur.execute("select aoptions.optext as \"options\" from aoptions join lang on lang.movie_id=aoptions.opid join movie on aoptions.opid=movie.movie_id where aoptions.opid!= \""+cid[j]+"\" and movie.m_release>\"1970\" AND movie.m_release<\"2020\" order by rand() limit 3;")
               
                opresult = cur.fetchall()
                opresult=list(opresult)
                opt1={'Name':'','Id':''}
                options=[]
                chars=[",","\"","\'"]
                i=0
                for x in opresult:
                    opt={'Name':'','Id':''}
                    x=str(x)
                    x=x.replace('(','')
                    x=x.replace(')','')
                    x=re.sub("|".join(chars),"",x)
                    opt['Name']=x
                    opt['Id']=i
                    options.append(opt)
                    i=i+1
                opt1['Name']=ans[j]
                opt1['Id']=4
                options.append(opt1)
                j=j+1        
                oprow_headers = [x[0] for x in cur.description]  # this will extract row headers
                qresult['options']=options
                del qresult['Answer']
                json_data.append(qresult)
            response={'IsSuccess':True,'error':'null','Data':[]}
            response['Data']=json_data
        else:
            start=[]
            end=[]
            def getStart(arg):
                switcher = {
                    "1": 1970,
                    "2": 1980,
                    "3": 1990,
                    "4": 2000,
                    "5": 2010
                }
                return switcher.get(str(arg),0)
            def getEnd(arg):
                switcher = {
                    "1": 1980,
                    "2": 1990,
                    "3": 2000,
                    "4": 2010,
                    "5": 2020
                }
                return switcher.get(arg,0)
            for d in decade:
                start.append(getStart(str(d)))
                end.append(getEnd(str(d)))
                
            
            if c==1:
                if len(region)==1:
                    cur.execute("SELECT image.image_URL as \"ImgUrl\",movie.movie_id as \"Qid\", movie.m_title as \"Answer\" FROM movie JOIN image ON movie.movie_id=image.movie_id join lang on movie.movie_id=lang.movie_id where lang.m_lang=\""+language+"\" AND movie.m_release>"+str(start[0])+" AND movie.m_release<"+str(end[-1])+" AND movie.movie_id not in (SELECT qid FROM user_questions WHERE user_id="+userid+") GROUP BY movie.movie_id order by rand() limit "+str(noq)+";")
                else:
                    cur.execute("SELECT image.image_URL as \"ImgUrl\",movie.movie_id as \"Qid\", movie.m_title as \"Answer\" FROM movie JOIN image ON movie.movie_id=image.movie_id join lang on movie.movie_id=lang.movie_id where movie.m_release>"+str(start[0])+" AND movie.m_release<"+str(end[-1])+" AND movie.movie_id not in (SELECT qid FROM user_questions WHERE user_id="+userid+") GROUP BY movie.movie_id order by rand() limit "+str(noq)+";")
            else:
                if len(region)==1:
                    cur.execute("SELECT image.image_URL as \"ImgUrl\",movie.movie_id as \"Qid\", movie.m_title as \"Answer\" FROM movie JOIN image ON movie.movie_id=image.movie_id join lang on movie.movie_id=lang.movie_id where lang.m_lang=\""+language+"\" AND movie.m_release>"+str(start[0])+" AND movie.m_release<"+str(end[-1])+" AND movie.movie_id not in (SELECT qid FROM user_questions WHERE user_id="+userid+") GROUP BY movie.movie_id order by rand() limit "+str(noq)+";")
                else:
                    cur.execute("SELECT image.image_URL as \"ImgUrl\",movie.movie_id as \"Qid\", movie.m_title as \"Answer\" FROM movie JOIN image ON movie.movie_id=image.movie_id join lang on movie.movie_id=lang.movie_id where movie.m_release>"+str(start[0])+" AND movie.m_release<"+str(end[-1])+" AND movie.movie_id not in (SELECT qid FROM user_questions WHERE user_id="+userid+") GROUP BY movie.movie_id order by rand() limit "+str(noq)+";")

            row_headers = [x[0] for x in cur.description]  # this will extract row headers
            myresult = cur.fetchall()
            myresult=list(myresult)
            json_data=[]
            cid=[]
            ans=[]
            j=0
            for i in myresult:
                qresult=dict(zip(row_headers,i))
                cid.append(qresult["Qid"])
                ans.append(qresult["Answer"])
            #options
                if len(region)==1:
                    cur.execute("select aoptions.optext as \"options\" from aoptions join lang on lang.movie_id=aoptions.opid join movie on aoptions.opid=movie.movie_id where aoptions.opid!= \""+cid[j]+"\" and lang.m_lang=\""+language+"\" AND movie.m_release>\""+str(start[0])+"\" AND movie.m_release<\""+str(end[-1])+"\" order by rand() limit 3;")
                else:
                    cur.execute("select aoptions.optext as \"options\" from aoptions join lang on lang.movie_id=aoptions.opid join movie on aoptions.opid=movie.movie_id where aoptions.opid!= \""+cid[j]+"\" and movie.m_release>\""+str(start[0])+"\" AND movie.m_release<\""+str(end[-1])+"\" order by rand() limit 3;")
                
                opresult = cur.fetchall()
                opresult=list(opresult)
                options=[]
                opt1={'Name':'','Id':''}
                i=0
                chars=[",","\"","\'"]
                for x in opresult:
                    opt={'Name':'','Id':''}
                    x=str(x)
                    x=x.replace('(','')
                    x=x.replace(')','')
                    x=re.sub("|".join(chars),"",x)
                    opt['Name']=x
                    opt['Id']=i
                    options.append(opt)
                    i=i+1
                opt1['Name']=ans[j]
                opt1['Id']=4
                options.append(opt1)
                j=j+1
                oprow_headers = [x[0] for x in cur.description]  # this will extract row headers
                qresult['options']=options
                del qresult['Answer']
                json_data.append(qresult)
            response={'IsSuccess':True,'error':'null','Data':[]}
            response['Data']=json_data
        cur.close()
        connection.close()
        return jsonify(response)
    except Exception as e:
        response={'IsSuccess':False,'error':str(e)}
        cur.close()
        connection.close()
        return jsonify(response)
   
   
    """#user status
        cur.execute("select usert.u_status as \"status\",achievements.u_coins as \"coins\" from usert join achievements on usert.user_id=achievements.user_id  where usert.user_id=\""+userid+"\"group by usert.user_id;")
        myresult1=cur.fetchall()
        row_headers1 = [x[0] for x in cur.description]  # this will extract row headers
        for x in myresult1:
            json_data.append(dict(zip(row_headers1,x)))"""


#Screen 8 & 9 Results API
@app.route('/result',methods=['POST'])
def viewResult():
    connection=pymysql.connect(**connection_properties)
    content=request.get_json()
    cur=connection.cursor()
    userid=content['userId']
    cc=content['Ccount']
    report=content['Report']
    qid=[]
    time=[]
    ic=[]
    c=0
    totalTime=0
    for x in report:
        qid.append(x['QID'])
        time.append(int(x['time']))
        ic.append(x['isCorrect']) 
    for x in time:
        totalTime=totalTime+x
        c=c+1  
    cans=int(cc)
    wans=len(ic)-cans
    totala=len(ic)
    avgtime=totalTime/c
    worsTime=max(time)
    bestTime=min(time)
    
    try:
        cur.execute("SELECT EXISTS(SELECT user_id FROM usert WHERE user_id="+userid+");")
        check=cur.fetchone()
        for x in check:
            print (x)
        if 0 in check:
            raise Exception("user does not exist")
        k=0
        for x in ic:
            if x==True:
                cur.execute("INSERT INTO user_questions (user_id,qid) VALUES("+userid+",\""+qid[k]+"\");")
                cur.execute("COMMIT")
                cur.execute("update user_questions join movie on  movie.movie_id=user_questions.qid set user_questions.era=movie.m_release where movie.movie_id=user_questions.qid and user_id="+str(userid)+";")
                cur.execute("COMMIT")
            k=k+1
        
        cur.execute("select u_coins,correct_images,u_level,wrong_images from achievements WHERE user_id=\""+userid+"\";")
        row_headers = [x[0] for x in cur.description]  # this will extract row headers
        myresult = cur.fetchall()
        for i in myresult:
            result_dict=dict(zip(row_headers,i))
        level=int(result_dict['u_level'])
        coins=int(result_dict['u_coins'])
        ci=int(result_dict['correct_images'])
        wi=int(result_dict['wrong_images'])
        coinsperlevel=0
        coins_earned=0
        ci=ci+cans
        wi=wi+wans
        if level == 1:
            coinsperlevel=10
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=25 and coins>=1000:
                level=level+1
        elif level == 2:
            coinsperlevel=10
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=75 and coins>=2250:
                level=level+1
        elif level == 3:
            coinsperlevel=12
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=150 and coins>=4800:
                level=level+1
        elif level == 4:     
            coinsperlevel=12
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=225 and coins>=7200:
                level=level+1
        elif level == 5:
            coinsperlevel=15
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=350 and coins>=11750:
                level=level+1
        elif level == 6:
            coinsperlevel=15
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=450 and coins>=14750:
                level=level+1
        elif level == 7:
            coinsperlevel=15
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=550 and coins>=18250:
                level=level+1
        elif level == 8:   
            coinsperlevel=18
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=675 and coins>=24150:
                level=level+1     
        elif level == 9:
            coinsperlevel=18
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=800 and coins>=28400:
                level=level+1
        elif level == 10:
            coinsperlevel=18
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=1000 and coins>=34000:
                level=level+1
        elif level == 11: 
            coinsperlevel=20
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=1200 and coins>=44000:
                level=level+1
        elif level == 12:
            coinsperlevel=20
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=1400 and coins>=52000:
                level=level+1
        elif level == 13:
            coinsperlevel=22
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=1600 and coins>=65200:
                level=level+1
        elif level == 14:
            coinsperlevel=22
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=1800 and coins>=72600:
                level=level+1
        elif level == 15: 
            coinsperlevel=25
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=2000 and coins>=90000:
                level=level+1
        elif level == 16:
            coinsperlevel=25
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=2200 and coins>=100000:
                level=level+1
        elif level == 17:
            coinsperlevel=25
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=2400 and coins>=112000:
                level=level+1
        elif level == 18:
            coinsperlevel=28
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=2600 and coins>=132800:
                level=level+1
        elif level == 19:
            coinsperlevel=28
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=2800 and coins>=148400:
                level=level+1
        elif level == 20: 
            coinsperlevel=30
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=3000 and coins>=180000:
                level=level+1

        cur.execute("UPDATE achievements SET u_level=\""+str(level)+"\",totalTime=\""+str(totalTime)+"\",u_coins=\""+str(coins)+"\",correct_images=\""+str(ci)+"\",wrong_images=wrong_images+\""+str(wi)+"\" WHERE user_id=\""+str(userid)+"\";")
        cur.execute("COMMIT")
        cur.execute("select u_level as \"level\" from achievements where user_id=\""+userid+"\"")
        myresult=cur.fetchall()
        row_headers = [x[0] for x in cur.description]
        res=[]
        for result in myresult:
            json_data=dict(zip(row_headers,result))
        json_data['coins']=coins_earned
        json_data['avgtime']=avgtime
        json_data['bestTime']=bestTime
        json_data['worstTime']=worsTime
        res.append(json_data)

        response={'IsSuccess':True,'error':'null','Data':res}
        cur.close()
        connection.close()
        return jsonify(response)
    except Exception as e:
        response={'IsSuccess':False,'error':str(e)}
        cur.close()
        connection.close()
        return jsonify(response)

#Screen 18 API link for Leaderboard Screen
@app.route('/leaderBoard/<userid>',methods=['GET'])
def lb(userid):
    try:
        connection=pymysql.connect(**connection_properties)
        cur=connection.cursor()
        #cur.execute("SELECT usert.user_name as \"NAME\", achievements.u_coins as \"Coins\", achievements.u_level as \"Level\", avatar.a_img_url as \"URL\" FROM usert JOIN achievements ON usert.user_id=achievements.user_id AND usert.region=\'"+region+"\' AND usert.city=\'"+city+"\' JOIN user_genre ON usert.user_id=user_genre.user_id AND user_genre.u_genre=\'"+genre+"\' JOIN avatar ON usert.avatar_id=avatar.avatar_id ORDER BY achievements.u_coins")
        cur.execute("SELECT user_id,u_level,u_coins,correct_images,wrong_images,totalTime from achievements where correct_images!=0;")
        row_headers = [x[0] for x in cur.description]  # this will extract row headers
        myresult = cur.fetchall()
        json_data = []
        cur.execute("select exists(select user_id from user_questions where user_id="+userid+");")
        check=cur.fetchone()
        c=0
        language=""
        if 0 in check:
            raise Exception("user does not exist.")
        else:
            c=1
        for result in myresult:
            json_data.append(dict(zip(row_headers, result)))
        
        user_id=[]
        level=[]
        coins=[]
        hitRatio=[]
        avgTime=[]

        for x in range(len(json_data)):
            user_id.append(int(json_data[x]['user_id']))
            level.append(int(json_data[x]['u_level']))
            coins.append(int(json_data[x]['u_coins']))
            correctQues=int(json_data[x]['correct_images'])
            wrongQues=int(json_data[x]['wrong_images'])
            totalQues=correctQues+wrongQues
            hRatio=correctQues/totalQues
            hitRatio.append(hRatio)
            timetotal=int(json_data[x]['totalTime'])
            avg_time=timetotal/totalQues
            avgTime.append(avg_time)
        import pandas as pd
        df=pd.DataFrame({'UserId':user_id,
                        'level':level,
                        'coins':coins,
                        'hitratio':hitRatio,
                        'avgtime':avgTime
        })
        df = df.sort_values(['level','coins','hitratio','avgtime'],ascending=[False,False,False,True])
        user=df.UserId.tolist()
        data=[]
        user_rank=user.index(int(userid))
        user_rank=user_rank+1
        for i in range(20):
            data_dict={'UserRank':i+1,'LeaderBoard':''}
            cur.execute("SELECT usert.user_id as \"Userid\",usert.screen_name as \"NAME\", achievements.u_coins as \"Coins\", achievements.u_level as \"Level\",achievements.silver as \"Silver\",achievements.bronze as \"Bronze\",achievements.gold as \"Gold\",achievements.u_crowns as \"Crown\", usert.img_url as \"URL\" FROM usert JOIN achievements ON usert.user_id=achievements.user_id where usert.user_id=\""+str(user[i])+"\";")
            row_headers = [x[0] for x in cur.description]  # this will extract row headers
            myresult = cur.fetchall()
            jsondata = []
            for result in myresult:
                jsondata.append(dict(zip(row_headers, result)))
            data_dict['LeaderBoard']=jsondata
            data.append(data_dict)
        data_dict={'UserRank':str(user_rank),'LeaderBoard':''}
        cur.execute("SELECT usert.user_id as \"Userid\", usert.screen_name as \"NAME\", achievements.u_coins as \"Coins\", achievements.u_level as \"Level\",achievements.silver as \"Silver\",achievements.bronze as \"Bronze\",achievements.gold as \"Gold\",achievements.u_crowns as \"Crown\" , usert.img_url as \"URL\" FROM usert JOIN achievements ON usert.user_id=achievements.user_id where usert.user_id=\""+str(userid)+"\";")
        row_headers = [x[0] for x in cur.description]  # this will extract row headers
        myresult = cur.fetchall()
        jsondata = []
        for result in myresult:
            jsondata.append(dict(zip(row_headers, result)))
        data_dict['LeaderBoard']=jsondata
        data.append(data_dict)
        response={'IsSuccess':True,'error':'null','Data':data}
        cur.close()
        connection.close()
        return jsonify(response)
    except Exception as e:
        response={'IsSuccess':False,'error':str(e)}
        cur.close()
        connection.close()
        return jsonify(response)   

#Screen 19 API link to earn coins through ads
@app.route('/earncoins',methods=['POST'])
def earncoins():
    connection=pymysql.connect(**connection_properties)
    cur=connection.cursor()
    content=request.get_json()
    userid=content["Id"]
    coins_add=content["CoinsToAdd"]
    resource_id=content["ResourceID"]
    credit=content["Credit"]
    debit=content["Debit"]
    try:
        if credit=="True":
            cur.execute("UPDATE achievements SET u_coins= u_coins + "+str(coins_add)+" WHERE user_id="+str(userid)+";")
            cur.execute("COMMIT;")
            cur.execute("UPDATE resources SET credit = credit + "+str(coins_add)+" WHERE rid="+str(resource_id)+";")
            cur.execute("COMMIT;")
            response={'IsSuccess':True,'error':'null'}
            cur.close()
            connection.close()
            return jsonify(response)
        elif debit=="True":
            cur.execute("UPDATE achievements SET u_coins= u_coins - "+str(coins_add)+" WHERE user_id="+str(userid)+";")
            cur.execute("COMMIT;")
            cur.execute("UPDATE resources SET debit = debit - "+str(coins_add)+" WHERE rid="+str(resource_id)+";")
            cur.execute("COMMIT;")
            response={'IsSuccess':True,'error':'null'}
            cur.close()
            connection.close()
            return jsonify(response)
    except Exception as e:
        response={'IsSuccess':False,'error':str(e)}
        cur.close()
        connection.close()
        return jsonify(response)

#get progressAPI
@app.route('/getProgress',methods=['POST'])
def getProgress():
    try:
        content=request.get_json()
        connection=pymysql.connect(**connection_properties)
        cur=connection.cursor()
        userid=content['UserId']
        region=content['Region']
        cur.execute("SELECT EXISTS(SELECT user_id FROM usert WHERE user_id="+str(userid)+");")
        check=cur.fetchone()
        if 0 in check:
            raise Exception("user does not exist")
        start=[1970,
            1980,
            1990,
            2000,
            2010]
        end=[1980,
            1990,
            2000,
            2010,
            2020]
        nOfCorrect=[]
        progress={}
        prog=[]
        i=1
        j=2
        for x in range(len(start)):
            if len(region)==2:
                cur.execute("select count(qid) as n from user_questions join lang on lang.movie_id=user_questions.qid where era>"+str(start[x])+" and era<"+str(end[x])+" and user_id="+str(userid)+";")
                row_headers = [x[0] for x in cur.description]
                myresult = cur.fetchone()
                n_correct=dict(zip(row_headers,myresult))
                correct=n_correct['n']
                cur.execute("select sum(era_total_questions) as n from era where era_id="+str(i)+" or era_id="+str(j)+";")
                i=i+2
                j=j+2    
                row_headers = [x[0] for x in cur.description]
                myresult = cur.fetchone()
                total_q=dict(zip(row_headers,myresult))
                total_ques=int(total_q['n'])
                progress["era "+str(x+1)]=correct/total_ques

            elif region[0]==1:
                language="English"
                cur.execute("select count(qid) as n from user_questions join lang on lang.movie_id=user_questions.qid where era>"+str(start[x])+" and era<"+str(end[x])+" and user_id="+str(userid)+" and lang.m_lang=\"English\";")
                row_headers = [x[0] for x in cur.description]
                myresult = cur.fetchone()
                n_correct=dict(zip(row_headers,myresult))
                correct=n_correct['n']
                cur.execute("select sum(era_total_questions) as n from era where era_id="+str(j)+";")
                j=j+2    
                row_headers = [x[0] for x in cur.description]
                myresult = cur.fetchone()
                total_q=dict(zip(row_headers,myresult))
                total_ques=int(total_q['n'])
                progress["era "+str(x+1)]=correct/total_ques

            elif region[0]==2:
                language="Hindi"
                cur.execute("select count(qid) as n from user_questions join lang on lang.movie_id=user_questions.qid where era>"+str(start[x])+" and era<"+str(end[x])+" and user_id="+str(userid)+" and lang.m_lang=\"Hindi\";")
                row_headers = [x[0] for x in cur.description]
                myresult = cur.fetchone()
                n_correct=dict(zip(row_headers,myresult))
                correct=n_correct['n']
                cur.execute("select sum(era_total_questions) as n from era where era_id="+str(i)+";")
                i=i+2    
                row_headers = [x[0] for x in cur.description]
                myresult = cur.fetchone()
                total_q=dict(zip(row_headers,myresult))
                total_ques=int(total_q['n'])
                progress["era "+str(x+1)]=correct/total_ques
            else:
                return "Enter valid Region"
        prog.append(progress)
        prog_response={'Id':userid,'Progress':prog}
        response={'IsSuccess':True,'Data':prog_response,'error':'null'}
        cur.close()
        connection.close()
        return jsonify(response)
    except Exception as e:
            response={'IsSuccess':False,'error':str(e)}
            cur.close()
            connection.close()
            return jsonify(response)
        

#MULTIUSER APIS
#Create room API
import uuid
@app.route('/createRoom',methods=['POST'])
def createroom():
    try:
        connection=pymysql.connect(**connection_properties)
        content=request.get_json()
        cur=connection.cursor()
        userid=content['UserId']

        cur.execute("SELECT EXISTS(SELECT user_id FROM usert WHERE user_id="+str(userid)+");")
        check=cur.fetchone()
        if 0 in check:
                raise Exception("user does not exist")
        nofusers=content['nofusers']
        noq=content["noQ"]
        region=content["Region"]
        if len(region)==2:
            language="Both"
        elif region[0]==1:
            language="English"
        elif region[0]==2:
            language="Hindi"
        #random alphanumeric roomid
        roomid=generateroomid()
        cur.execute("insert into room(room_id,host,no_of_users_allowed,game_status,region,noQ) values(\""+roomid+"\",\""+str(userid)+"\",\""+str(nofusers)+"\",\"created\",\""+language+"\",\""+str(noq)+"\"); ")
        cur.execute("insert into room_users(room_id,user_id) values(\""+roomid+"\",\""+userid+"\");")
        cur.execute("commit")
        data={'RoomId':roomid}
        cur.execute("SELECT screen_name,img_url from usert where user_id=\""+str(userid)+"\";")       
        row_headers = [x[0] for x in cur.description]
        myresult = cur.fetchall()
        for result in myresult:
            room_users=dict(zip(row_headers, result))
            room_users['User_id']=userid
        user_data=json.dumps(room_users)
        user_data=str(user_data)
        response={'IsSuccess':True,'Data':data,'error':'null'} 
        #Storage of questions
        cur.execute("SELECT region,noQ from room where room_id=\""+roomid+"\"; ")
        row_headers = [x[0] for x in cur.description]
        myresult = cur.fetchone()
        x=dict(zip(row_headers,myresult))
        language=x['region']
        noq=x['noQ']
        cur.execute("select user_id from room_users where room_id=\""+roomid+"\";")
        myresult = cur.fetchall()
        myresult=list(myresult)
        chars=[",","\"","\'"]
        i=0
        users=[]
        for x in myresult:
            opt={'Name':'','Id':''}
            x=str(x)
            x=x.replace('(','')
            x=x.replace(')','')
            x=re.sub("|".join(chars),"",x)
            users.append(x)

        if language=="Both":
            cur.execute("SELECT image.image_URL as \"ImgUrl\",movie.movie_id as \"Qid\", movie.m_title as \"Answer\" FROM movie JOIN image ON movie.movie_id=image.movie_id join lang on movie.movie_id=lang.movie_id GROUP BY movie.movie_id order by rand() limit "+str(noq)+";")
        else:
            cur.execute("SELECT image.image_URL as \"ImgUrl\",movie.movie_id as \"Qid\", movie.m_title as \"Answer\" FROM movie JOIN image ON movie.movie_id=image.movie_id join lang on movie.movie_id=lang.movie_id where lang.m_lang=\""+language+"\" GROUP BY movie.movie_id order by rand() limit "+str(noq)+";")       

        row_headers = [x[0] for x in cur.description]  # this will extract row headers
        myresult = cur.fetchall()
        myresult=list(myresult)
        json_data=[]
        cid=[]
        ans=[]
        data=[]
        j=0
        m=0
        for i in myresult:
            qresult=dict(zip(row_headers,i))
            cid.append(qresult["Qid"])
            ans.append(qresult["Answer"])
            
            #options
            if language=="Both":
                cur.execute("select aoptions.optext as \"options\" from aoptions join lang on lang.movie_id=aoptions.opid join movie on aoptions.opid=movie.movie_id where aoptions.opid!= \""+cid[j]+"\" and movie.m_release>\"1970\" AND movie.m_release<\"2020\" order by rand() limit 3;")
            else:
                cur.execute("select aoptions.optext as \"options\" from aoptions join lang on lang.movie_id=aoptions.opid join movie on aoptions.opid=movie.movie_id where aoptions.opid!= \""+cid[j]+"\" and lang.m_lang=\""+language+"\" AND movie.m_release>\"1970\" AND movie.m_release<\"2020\" order by rand() limit 3;")   
            opresult = cur.fetchall()
            opresult=list(opresult)
            opt1={'Name':'','Id':''}
            options=[]
            chars=[",","\"","\'"]
            i=1
            room_op=[]
            for x in opresult:
                opt={'Name':'','Id':''}
                x=str(x)
                x=x.replace('(','')
                x=x.replace(')','')
                x=re.sub("|".join(chars),"",x)
                opt['Name']=x
                room_op.append(x)
                opt['Id']=i
                options.append(opt)
                i=i+1
            
            opt1['Name']=ans[j]
            opt1['Id']=4
            options.append(opt1)
            j=j+1        
            oprow_headers = [x[0] for x in cur.description]  # this will extract row headers
            qresult['options']=options
            del qresult['Answer']
            json_data.append(qresult)
            for k in room_op:
                cur.execute("INSERT INTO room_options(room_id,q_id,op_id) VALUES(\'"+roomid+"\',\'"+cid[m]+"\',\'"+k+"\');")
                cur.execute("COMMIT")
            m=m+1
        
        """x_result={'RoomId':roomid,'Users':users,'gameStatus':'Started'}
        response={'IsSuccess':True,'error':'null','Data':[]}
        x_result['Questions']=json_data
        data.append(x_result)
        response['Data']=data
        cur.execute("update room set game_status=\"Started\" where room_id=\""+roomid+"\";")
        cur.execute("COMMIT")"""
        x=1
        for qid in cid:
            cur.execute("INSERT INTO room_result(room_id,q_id,sort_id) VALUES(\'"+roomid+"\',\'"+qid+"\',\'"+str(x)+"\');")
            cur.execute("COMMIT")
            x=x+1
        #firebase
        from firebase import firebase
        fire=firebase.FirebaseApplication("https://filmybuff-test.firebaseio.com/", None)
        datatofire={
            '1':user_data,
            'HasStarted':False
        }
        result=fire.patch('/room/'+roomid+'/',datatofire)
        from firebase import firebase
        fire=firebase.FirebaseApplication("https://filmybuff-test.firebaseio.com/", None)

        """for i in range(1,noq+1):
            for x in range(1,nofusers+1):
                datax={
                    'hasAnswered':0,
                    'isCorrect':0,
                    'time':0
                }
                result=fire.patch('/questions/'+roomid+'/'+str(i)+'/'+str(x),datax) """
       
        datax={
            "QuestionNo":0,
            "UsersAnswered":0,
            "ReportGenerated":False,
            "lastestAnsweredCorrect":0,
            "lastestAnsweredWrong":0
        }
        result=fire.patch('/questions/'+roomid+'/OtherInfo',datax)
        #response={'IsSuccess':True,'Data':data,'error':'null'} 

        return jsonify(response)
    except Exception as e:
        response={'IsSuccess':False,'error':str(e)}
        return jsonify(response)
#roomid generator
def generateroomid():
    roomid = str(uuid.uuid4())
    roomid= roomid[0:8]
    connection=pymysql.connect(**connection_properties)
    cur=connection.cursor()
    cur.execute("SELECT room_id from room;")
    row_headers = [x[0] for x in cur.description]
    myresult = cur.fetchall()
    json_data = []
    for result in myresult:
        json_data.append(dict(zip(row_headers, result)))   
    roomids=[]
    for i in json_data:
        roomids.append(i['room_id'])
    for i in roomids:
        if roomid==i:
            roomid=generateroomid() 
        else:
            break
    cur.close()
    connection.close()
    return roomid.lower()

#Join room API
@app.route('/joinRoom',methods=['POST'])
def joinroom():
    try:
        content=request.get_json()
        connection=pymysql.connect(**connection_properties)
        cur=connection.cursor()
        userid=content['UserId'] 
        roomid=content['RoomId']
        cur.execute("SELECT EXISTS(SELECT user_id FROM usert WHERE user_id="+str(userid)+");")
        check=cur.fetchone()
        if 0 in check:
            raise Exception("user does not exist")
        cur.execute("SELECT no_of_users_joined as j,no_of_users_allowed as a from room where room_id=\""+roomid+"\"; ")
        row_headers = [x[0] for x in cur.description]
        myresult = cur.fetchone()
        x=dict(zip(row_headers,myresult))
        joined=x['j']
        allowed=x['a']
        cur.execute("select user_id from room_users where room_id=\""+roomid+"\";")
        myresult = cur.fetchall()
        myresult=list(myresult)
        chars=[",","\"","\'"]
        users=[]
        for x in myresult:
            opt={'Name':'','Id':''}
            x=str(x)
            x=x.replace('(','')
            x=x.replace(')','')
            x=re.sub("|".join(chars),"",x)
            users.append(x)
        check={'Result':""}
        if joined==allowed or joined>allowed:
            raise Exception("Room Full.")
        else:
            cur.execute("insert into room_users(room_id,user_id) values(\""+roomid+"\",\""+str(userid)+"\");")
            cur.execute("COMMIT")
            cur.execute("update room set no_of_users_joined=no_of_users_joined+1 where room_id=\""+roomid+"\";")
            cur.execute("COMMIT")
            check['Result']="Success."
        cur.execute("select user_id from room_users where room_id=\""+roomid+"\";")
        myresult = cur.fetchall()
        myresult=list(myresult)
        chars=[",","\"","\'"]
        users=[]
        for x in myresult:
            opt={'Name':'','Id':''}
            x=str(x)
            x=x.replace('(','')
            x=x.replace(')','')
            x=re.sub("|".join(chars),"",x)
            users.append(x)
        present_users=[]
        for u in users:
            cur.execute("SELECT screen_name,img_url from usert where user_id=\""+str(u)+"\";")       
            row_headers = [x[0] for x in cur.description]
            myresult = cur.fetchall()
            for result in myresult:
                room_users=dict(zip(row_headers, result))
                room_users['User_id']=u
                present_users.append(room_users)
        cur.execute("select host from room where room_id=\""+roomid+"\";")
        myresult = cur.fetchone()
        myresult=list(myresult)
        chars=[",","\"","\'"]
        
        for x in myresult:
            opt={'Name':'','Id':''}
            x=str(x)
            x=x.replace('(','')
            x=x.replace(')','')
            x=re.sub("|".join(chars),"",x)
        check['Host']=x
        check['Users']=present_users
        check['RoomId']=roomid
        response={'IsSuccess':True,'error':'null','Data':[]}
        response['Data']=check
        cur.execute("SELECT screen_name,img_url from usert where user_id=\""+str(userid)+"\";")       
        row_headers = [x[0] for x in cur.description]
        myresult = cur.fetchall()
        for result in myresult:
            room_users=dict(zip(row_headers, result))
            room_users['User_id']=userid
        user_data=json.dumps(room_users)
        user_data=str(user_data)
        #Firebase
        from firebase import firebase
        fire=firebase.FirebaseApplication("https://filmybuff-test.firebaseio.com/", None)
        x=fire.get('/room',roomid)
        del x["HasStarted"]
        keys=list(x.keys())
        x=keys[-1]
        x=int(x)
        data={
            x+1:user_data,
        }
        result=fire.patch('/room/'+roomid+'/',data) 

        connection.close()
        cur.close()
        return jsonify(response)

    except Exception as e:
        response={'IsSuccess':False,'error':str(e)}
        cur.close()
        connection.close()
        return jsonify(response)

#Start multiplayer game
@app.route('/startMultiPlayerGame',methods=['POST'])
def startmultiplayergame():
    try:
        content=request.get_json()
        connection=pymysql.connect(**connection_properties)
        cur=connection.cursor()
        roomid=content['RoomId']
        cur.execute("SELECT region,noQ from room where room_id=\""+roomid+"\"; ")
        row_headers = [x[0] for x in cur.description]
        myresult = cur.fetchone()
        x=dict(zip(row_headers,myresult))
        language=x['region']
        noq=x['noQ']
        cur.execute("select user_id from room_users where room_id=\""+roomid+"\";")
        myresult = cur.fetchall()
        myresult=list(myresult)
        chars=[",","\"","\'"]
        i=0
        users=[]
        for x in myresult:
            opt={'Name':'','Id':''}
            x=str(x)
            x=x.replace('(','')
            x=x.replace(')','')
            x=re.sub("|".join(chars),"",x)
            users.append(x)

        cur.execute("SELECT room_result.sort_id as \"SortId\", image.image_URL as \"ImgUrl\",movie.movie_id as \"Qid\", movie.m_title as \"Answer\" FROM movie JOIN image ON movie.movie_id=image.movie_id JOIN room_result on movie.movie_id=room_result.q_id WHERE room_result.room_id=\""+roomid+"\"GROUP BY movie.movie_id order by rand() limit "+str(noq)+";")
        row_headers = [x[0] for x in cur.description]  # this will extract row headers
        myresult = cur.fetchall()
        myresult=list(myresult)
        json_data=[]
        cid=[]
        ans=[]
        data=[]
        j=0
        m=0
        for i in myresult:
            qresult=dict(zip(row_headers,i))
            cid.append(qresult["Qid"])
            ans.append(qresult["Answer"])

            #options
            cur.execute("SELECT op_id FROM movietmdb.room_options WHERE movietmdb.room_options.q_id=\'"+cid[m]+"\' AND movietmdb.room_options.room_id=\'"+roomid+"\';")
            opresult = cur.fetchall()
            opresult=list(opresult)
            

            opt1={'Name':'','Id':''}
            options=[]
            chars=[",","\"","\'"]
            i=1
            for x in opresult:
                opt={'Name':'','Id':''}
                x=str(x)
                x=x.replace('(','')
                x=x.replace(')','')
                x=re.sub("|".join(chars),"",x)
                opt['Name']=x
                opt['Id']=i
                options.append(opt)
                i=i+1
            
            
            opt1['Name']=ans[j]
            opt1['Id']=4
            options.append(opt1)
            j=j+1        
            oprow_headers = [x[0] for x in cur.description]  # this will extract row headers
            qresult['options']=options
            del qresult['Answer']
            json_data.append(qresult)
            m=m+1

        x_result={'RoomId':roomid,'Users':users,'gameStatus':'Started'}
        response={'IsSuccess':True,'error':'null','Data':[]}
        x_result['Questions']=json_data
        data.append(x_result)
        response['Data']=data
        cur.execute("update room set game_status=\"Started\" where room_id=\""+roomid+"\";")
        cur.execute("COMMIT")
        

        cur.close()
        connection.close()

        #firebase
        from firebase import firebase
        fire=firebase.FirebaseApplication("https://filmybuff-test.firebaseio.com/", None)
        """x=1
        for i in json_data:
            if x<=noq:
                data={
                    'question':str(i)
                }
                result=fire.patch('/questions/'+roomid+'/'+str(x),data) 
                x=x+1"""
        datax={
            'HasStarted':True,
        }
        result=fire.patch('/room/'+roomid+'/',datax)
        return jsonify(response)
    except Exception as e:
        response={'IsSuccess':False,'error':str(e)}
        cur.close()
        connection.close()
        return jsonify(response)    

#end multiplayer game
@app.route('/endGame',methods=['POST'])
def endGame():
    try:    
        connection=pymysql.connect(**connection_properties)
        content=request.get_json()
        cur=connection.cursor()
        roomid=content['RoomId']
        #report=content['Report']
        #noq=content['noQ']
        qid=[]
        time=[]
        ic=[]
        uid=[]
        c=0
        res=[]
        totalTime=0
        
        """for x in range(len(report)):
            qid.append(report[x]['QID'])
            time.append(int(report[x]['time']))
            ic.append(report[x]['isCorrect'])"""
        #Get all lists here from firebase
        from firebase import firebase
        fire=firebase.FirebaseApplication("https://filmybuff-test.firebaseio.com/", None)
        #result=fire.delete('/room',roomid)
        result=fire.get('/questions/'+roomid,'reports')
        qid=list(result.keys())
        uid=list(result[qid[0]].keys())

        noq=len(qid)
        for i in qid:
            for j in uid:
                time.append(result[i][j]['Time'])
                ic.append(result[i][j]['isCorrect'])
                
                if result[i][j]['isCorrect']==True:
                    cur.execute("UPDATE room_result SET isCorrect = concat(isCorrect, \',"+j+"\') where room_id=\'"+roomid+"\' and q_id=\'"+i+"\';")
                    cur.execute("COMMIT;")
                    
                else:
                    cur.execute("UPDATE room_result SET isWrong = concat(isWrong, \',"+j+"\') where room_id=\'"+roomid+"\' and q_id=\'"+i+"\';")
                    cur.execute("COMMIT;")

                #j=j+1
        
        for i in range(len(ic)):
            if ic[i]==True:
                ic[i]=1
            else:
                ic[i]=0
        
        """for x in report:
            uid.append(x['UserId'])
        uid= list(dict.fromkeys(uid))"""
        
        x=0 
        user_dict={"UserId "+uid[x]:[]} 
        u_report={"UserId "+uid[x]:[]} 

        for user in range(len(uid)):
            u_qid=[]
            u_time=[]
            u_ic=[]  
            user_details=[]    
            x=0

            for i in range(noq):
                user_details.append(qid[x])
                user_details.append(time[x])
                user_details.append(ic[x])
                time.remove(time[0])
                ic.remove(ic[0])
            user_dict["UserId "+uid[x]]=user_details
            t=1
            q=0
            c=2
            cc=0
            for x in range(noq):
                u_qid.append(user_details[q])
                u_time.append(user_details[t])
                u_ic.append(user_details[c])
                t=t+3
                q=q+3
                c=c+3
            c=0
            
            for x in u_ic:
                if x==1:
                    cc=cc+1
            for x in u_time:
                c=c+1  
            totalTime=sum(u_time)
            cans=int(cc)
            wans=len(u_ic)-cans
            totala=len(u_ic)
            avgtime=totalTime/c
            worsTime=max(u_time)
            bestTime=min(u_time)
            x=0
            cur.execute("SELECT EXISTS(SELECT user_id FROM usert WHERE user_id="+uid[x]+");")
            check=cur.fetchone()
            if 0 in check:
                raise Exception("user does not exist")
            
            cur.execute("select u_coins,correct_images,u_level,wrong_images from achievements WHERE user_id=\""+uid[x]+"\";")
            row_headers = [x[0] for x in cur.description]  # this will extract row headers
            myresult = cur.fetchall()
            for i in myresult:
                result_dict=dict(zip(row_headers,i))
            
            level=int(result_dict['u_level'])
            coins=int(result_dict['u_coins'])
            ci=int(result_dict['correct_images'])
            wi=int(result_dict['wrong_images'])
            
            coins_earned=0
            ci=ci+cans
            wi=wi+wans
            if level == 1:
                coinsperlevel=10
                coins_earned=coinsperlevel*cans
                coins=coins+coins_earned
                if ci>=25 and coins>=1000:
                    level=level+1
            elif level == 2:
                coinsperlevel=10
                coins_earned=coinsperlevel*cans
                coins=coins+coins_earned
                if ci>=75 and coins>=2250:
                    level=level+1
            elif level == 3:
                coinsperlevel=12
                coins_earned=coinsperlevel*cans
                coins=coins+coins_earned
                if ci>=150 and coins>=4800:
                    level=level+1
            elif level == 4:     
                coinsperlevel=12
                coins_earned=coinsperlevel*cans
                coins=coins+coins_earned
                if ci>=225 and coins>=7200:
                    level=level+1
            elif level == 5:
                coinsperlevel=15
                coins_earned=coinsperlevel*cans
                coins=coins+coins_earned
                if ci>=350 and coins>=11750:
                    level=level+1
            elif level == 6:
                coinsperlevel=15
                coins_earned=coinsperlevel*cans
                coins=coins+coins_earned
                if ci>=450 and coins>=14750:
                    level=level+1
            elif level == 7:
                coinsperlevel=15
                coins_earned=coinsperlevel*cans
                coins=coins+coins_earned
                if ci>=550 and coins>=18250:
                    level=level+1
            elif level == 8:   
                coinsperlevel=18
                coins_earned=coinsperlevel*cans
                coins=coins+coins_earned
                if ci>=675 and coins>=24150:
                    level=level+1     
            elif level == 9:
                coinsperlevel=18
                coins_earned=coinsperlevel*cans
                coins=coins+coins_earned
                if ci>=800 and coins>=28400:
                    level=level+1
            elif level == 10:
                coinsperlevel=18
                coins_earned=coinsperlevel*cans
                coins=coins+coins_earned
                if ci>=1000 and coins>=34000:
                    level=level+1
            elif level == 11: 
                coinsperlevel=20
                coins_earned=coinsperlevel*cans
                coins=coins+coins_earned
                if ci>=1200 and coins>=44000:
                    level=level+1
            elif level == 12:
                coinsperlevel=20
                coins_earned=coinsperlevel*cans
                coins=coins+coins_earned
                if ci>=1400 and coins>=52000:
                    level=level+1
            elif level == 13:
                coinsperlevel=22
                coins_earned=coinsperlevel*cans
                coins=coins+coins_earned
                if ci>=1600 and coins>=65200:
                    level=level+1
            elif level == 14:
                coinsperlevel=22
                coins_earned=coinsperlevel*cans
                coins=coins+coins_earned
                if ci>=1800 and coins>=72600:
                    level=level+1
            elif level == 15: 
                coinsperlevel=25
                coins_earned=coinsperlevel*cans
                coins=coins+coins_earned
                if ci>=2000 and coins>=90000:
                    level=level+1
            elif level == 16:
                coinsperlevel=25
                coins_earned=coinsperlevel*cans
                coins=coins+coins_earned
                if ci>=2200 and coins>=100000:
                    level=level+1
            elif level == 17:
                coinsperlevel=25
                coins_earned=coinsperlevel*cans
                coins=coins+coins_earned
                if ci>=2400 and coins>=112000:
                    level=level+1
            elif level == 18:
                coinsperlevel=28
                coins_earned=coinsperlevel*cans
                coins=coins+coins_earned
                if ci>=2600 and coins>=132800:
                    level=level+1
            elif level == 19:
                coinsperlevel=28
                coins_earned=coinsperlevel*cans
                coins=coins+coins_earned
                if ci>=2800 and coins>=148400:
                    level=level+1
            elif level == 20: 
                coinsperlevel=30
                coins_earned=coinsperlevel*cans
                coins=coins+coins_earned
                if ci>=3000 and coins>=180000:
                    level=level+1
            
            cur.execute("UPDATE achievements SET u_level=\""+str(level)+"\",totalTime=\""+str(totalTime)+"\",u_coins=\""+str(coins)+"\",correct_images=\""+str(ci)+"\",wrong_images=\""+str(wi)+"\" WHERE user_id=\""+str(uid[x])+"\";")
            cur.execute("COMMIT")
            
            cur.execute("select u_level as \"level\" from achievements where user_id=\""+uid[x]+"\"")
            myresult=cur.fetchall()
            row_headers = [x[0] for x in cur.description]
            for result in myresult:
                json_data=dict(zip(row_headers,result))
            
            json_data['coins']=coins_earned
            json_data['avgtime']=avgtime
            json_data['bestTime']=bestTime
            json_data['worstTime']=worsTime
            json_data['UserId']=uid[x]
            json_data['CorrectAnswered']=cans
            
            cur.execute("SELECT screen_name,img_url from usert where user_id=\""+str(uid[x])+"\";")       
            row_headers = [x[0] for x in cur.description]
            myresult = cur.fetchall()
            for result in myresult:
                room_users=dict(zip(row_headers, result))
            json_data['ScreenName']=room_users['screen_name']
            json_data['ImgUrl']=room_users['img_url']
            res.append(json_data)
            uid.remove(uid[0])
            x=0
        cur.execute("update room set game_status=\"Ended\" where room_id=\""+roomid+"\";")
        cur.execute("COMMIT")
        cur.close()
        connection.close()
        result=fire.put('/questions/'+roomid+'/OtherInfo',"ReportGenerated",True)
        response={'IsSuccess':True,'error':'null','Data':res}
        return jsonify(response)    

    except Exception as e:
        response={'IsSuccess':False,'error':str(e)}
        cur.close()
        connection.close()
        return jsonify(response)  


#Delete API
@app.route('/delete',methods=['POST'])
def delete():
    try:    
        connection=pymysql.connect(**connection_properties)
        content=request.get_json()
        cur=connection.cursor()
        roomid=content['RoomId']
        userid=content['UserId']
        cur.execute("SELECT EXISTS(SELECT user_id FROM room_users WHERE user_id="+str(userid)+" and room_id=\""+roomid+"\");")
        check=cur.fetchone()
        if 0 in check:
            raise Exception("user does not exist")
        cur.execute("Delete from room_users where room_id=\'"+roomid+"\' and user_id=\'"+userid+"\';")
        cur.execute("update room set no_of_users_joined=no_of_users_joined-1 where room_id=\""+roomid+"\";")
        cur.execute("COMMIT")
        response={'IsSuccess':True,'error':'null','Data':'Success'}
        cur.close()
        connection.close()
        from firebase import firebase
        fire=firebase.FirebaseApplication("https://filmybuff-test.firebaseio.com/", None)
        result=fire.get('/room',roomid)
        del result['HasStarted']
        j=1
        keys=list(result.keys())
        for i in range(len(keys)):
            z=json.loads(result[str(keys[i])])
            if z['User_id']==userid:
                fire.delete("/room/"+roomid,j)
            j=j+1
        return jsonify(response)

    except Exception as e:
        response={'IsSuccess':False,'error':str(e)}
        cur.close()
        connection.close()
        return jsonify(response) 

if __name__ == '__main__':
    app.run(debug=True,port=9999)

