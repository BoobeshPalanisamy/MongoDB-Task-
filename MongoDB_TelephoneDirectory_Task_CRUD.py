#import necessary modules
import pymongo

#Create a database 
myclient=pymongo.MongoClient("mongodb://127.0.0.1:27017")
db = myclient['telephonedirectory']

#Create a collection
mycollection = db['details']

#create directory
info=[
      {'name':'Ram','ph':'9600688876','place':'Adyar','city':'Chennai'},
      {'name':'Shankar','ph':'9600688545','place':'Thambaram','city':'Coimbatore'},
      {'name':'Kannan','ph':'9600688752','place':'Chrompet','city':'Chennai'},
      {'name':'Swetha','ph':'9600682545','place':'Guindy','city':'Madurai'},
      {'name':'Ponni','ph':'9600785265','place':'Besantnagar','city':'Chennai'},
      {'name':'Samy','ph':'9600688885','place':'Alwarpet','city':'Madurai'},
      {'name':'Kavitha','ph':'9600689975','place':'Pallavaram','city':'Chennai'},
      {'name':'Ponni','ph':'9600777875','place':'Tharamani','city':'Chennai'},
      {'name':'Dinesh','ph':'9600685655','place':'Annanagar','city':'Coimbatore'},
      {'name':'Hari','ph':'9602545875','place':'Ashoknagar','city':'Chennai'},
      {'name':'Anil','ph':'9842732887','place':'Coyambedu','city':'Chennai'},
      {'name':'Arjun','ph':'9952314587','place':'Saibabacolony','city':'Coimbatore'},
      {'name':'Kannan','ph':'6378954525','place':'Chrompet','city':'Chennai'},
      {'name':'Bhuvana','ph':'6325457895','place':'Mattuthavani','city':'Madurai'},
      {'name':'Ponni','ph':'7852546523','place':'Besantnagar','city':'Chennai'},
      ]

#Insert the record into the collection
mycollection.insert_many(info)


#Find records by queries
data = mycollection.find({'place':'Chrompet'})
for records in data:
    print(records)
    

#Update the data using update_one() 
data={'ph':'9600688752'}
new_data={'$set':{'name':'Arul'}}
mycollection.update_one(data,new_data)



#Delete the data using delete_one()
data = {'name':'Ponni'}
mycollection.delete_one(data)













