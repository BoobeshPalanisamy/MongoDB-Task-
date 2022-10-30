import pymongo
import json
import pandas as pd

#create a database
myclient=pymongo.MongoClient("mongodb://127.0.0.1:27017")
mydb=myclient['students']
mycollection = mydb['marks']


#load the student.json dataset
data=[]
with open('students.json','r') as a:
    for line in a:
        data.append(json.loads(line))
        
        
#Insert the students record into the collection
mycollection.insert_many(data)




#1)Find the student name who scored maximum scores in all (exam, quiz and homework)?
exam=[]
quiz=[]
homework=[]
for mark in range(0,len(data)):
    exam.append(data[mark]["scores"][0]["score"])
    quiz.append(data[mark]["scores"][1]["score"])
    homework.append(data[mark]["scores"][2]["score"])
print(data[exam.index(max(exam))]['name'],'score max mark in exam',max(exam))
print(data[quiz.index(max(quiz))]['name'],'score max mark in quiz',max(quiz))
print(data[homework.index(max(homework))]['name'],'score max mark in h_w',max(homework)) 




#2)Find students who scored below average in the exam
exam_marks=[]
below_avg=[]
for marks in range(0,len(data)):
    exam_marks.append(data[marks]["scores"][0]["score"])
average=sum(exam_marks)/len(exam_marks)
for i in exam_marks:
  if i<average:
    below_avg.append(data[exam_marks.index(i)])
for j in below_avg:
    print(j)





#3)Find students who scored below pass mark and assigned them as fail, and above pass mark as pass in all the categories.
df = pd.DataFrame(columns=['_id','name','exam_score','quiz_score','hw_score'])
for i in range(0,len(data)):
  df.loc[i] = [data[i]['_id'],data[i]['name'],data[i]['scores'][0]['score'],
               data[i]['scores'][1]['score'],data[i]['scores'][2]['score']]
result=[]
for mark in range(0,len(data)):
  if (data[mark]["scores"][0]["score"])>=40 and (data[mark]["scores"][1]["score"])>=40 and (data[mark]["scores"][2]["score"])>=40:
    result.append("Pass")
  else:
    result.append("Fail")
df["result"]=result
print(df)




#4)Find the total and average of the exam, quiz and homework and store them in a separate collection.
total_avg_collection = mydb["tot_avg"]
total_average=[]
for row in mycollection.aggregate([{"$project": { "name": 1 ,
                                                 "totalscore":{"$sum":"$scores.score"},
                                                 "average":{"$avg":"$scores.score"}}}]):
    total_average.append(row)
tot_avg = [i for i in total_average]
total_avg_collection.insert_many(tot_avg)




#5)Create a new collection which consists of students who scored below average and above 40% in all the categories.
below_avg_pass=mydb["avg_pass"]
df = pd.DataFrame(columns=['_id','name','exam_score','quiz_score','hw_score'])
for i in range(0,len(data)):
    df.loc[i] = [data[i]['_id'],data[i]['name'],data[i]['scores'][0]['score'],
                 data[i]['scores'][1]['score'],data[i]['scores'][2]['score']]
bavg_students= df[(df['exam_score'] >= 40) & (df['exam_score'] < (df['exam_score'].mean())) &
                  (df['quiz_score'] >= 40) & (df['quiz_score'] < (df['quiz_score'].mean())) &
                  (df['hw_score'] >= 40) & (df['hw_score'] < (df['hw_score'].mean()))]
bavg_students_index = list(bavg_students.index)
bavg_data = [data[i] for i in bavg_students_index] 
below_avg_pass.insert_one(bavg_data)
#Given condition resulted empty list so cant insert into collection



#6)Create a new collection which consists of students who scored below the fail mark in all the categories.
below_fail=mydb["below_fail"]
df = pd.DataFrame(columns=['_id','name','exam_score','quiz_score','hw_score'])
for i in range(0,len(data)):
    df.loc[i] = [data[i]['_id'],data[i]['name'],data[i]['scores'][0]['score'],
                 data[i]['scores'][1]['score'],data[i]['scores'][2]['score']]

fail_students = df[(df['exam_score'] < 40) & (df['quiz_score'] < 40) & (df['hw_score'] < 40)]
fail_students_index = list(fail_students.index)
fail_data = [data[i] for i in fail_students_index] 
below_fail.insert_many(fail_data)




#7)Create a new collection which consists of students who scored above pass mark in all the categories.
above_pass=mydb["above_pass"]
df = pd.DataFrame(columns=['_id','name','exam_score','quiz_score','hw_score'])
for i in range(0,len(data)):
    df.loc[i] = [data[i]['_id'],data[i]['name'],data[i]['scores'][0]['score'],
                 data[i]['scores'][1]['score'],data[i]['scores'][2]['score']]

pass_students = df[(df['exam_score'] >= 40) & (df['quiz_score'] >= 40) & (df['hw_score'] >= 40)]
pass_students_index = list(pass_students.index)
pass_data = [data[i] for i in pass_students_index] 
above_pass.insert_many(pass_data)

































    