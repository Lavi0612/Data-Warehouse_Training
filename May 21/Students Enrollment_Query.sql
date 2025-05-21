 Exercise: Student Enrollments System 
 Database: campusdb 
 Collection: enrollments 

1. Use a new database called campusdb 

use campusdb

Output:

switched to db campusdb

2. Create a collection enrollments and insert 4 student documents. Each document should include: 
name (string) 
studentId (number) 
courses (array of strings) 
address (sub-document with city , state ) 
feesPaid (boolean) 
Ensure: 
At least 1 student is from "Delhi" 
At least 2 students are enrolled in "Python" 
At least 1 student has feesPaid: false 

db.enrollments.insertMany([ 
  { 
    name: "Ananya Verma", 
    studentId: 101, 
    courses: ["Python", "Java"], 
    address: { city: "Delhi", state: "Delhi" }, 
    feesPaid: true 
  }, 
  { 
    name: "Rohan Mehta", 
    studentId: 102, 
    courses: ["Python", "AI"], 
    address: { city: "Bangalore", state: "Karnataka" }, 
    feesPaid: false 
  }, 
  { 
    name: "Sneha Kapoor", 
    studentId: 103, 
    courses: [], 
    address: { city: "Hyderabad", state: "Telangana" }, 
    feesPaid: true 
  }, 
  { 
    name: "Imran Shaikh", 
    studentId: 104, 
    courses: ["Data Science", "Java"], 
    address: { city: "Delhi", state: "Delhi" }, 
    feesPaid: false 
  } 
])

Output:

{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('682d54269101f6a1cfbe9431'),
    '1': ObjectId('682d54269101f6a1cfbe9432'),
    '2': ObjectId('682d54269101f6a1cfbe9433'),
    '3': ObjectId('682d54269101f6a1cfbe9434')
  }
}

3. Display all student records 

db.enrollments.find()

Output:

{
  _id: ObjectId('682d54269101f6a1cfbe9431'),
  name: 'Ananya Verma',
  studentId: 101,
  courses: [
    'Python',
    'Java'
  ],
  address: {
    city: 'Delhi',
    state: 'Delhi'
  },
  feesPaid: true
}
{
  _id: ObjectId('682d54269101f6a1cfbe9432'),
  name: 'Rohan Mehta',
  studentId: 102,
  courses: [
    'Python',
    'AI'
  ],
  address: {
    city: 'Bangalore',
    state: 'Karnataka'
  },
  feesPaid: false
}
{
  _id: ObjectId('682d54269101f6a1cfbe9433'),
  name: 'Sneha Kapoor',
  studentId: 103,
  courses: [],
  address: {
    city: 'Hyderabad',
    state: 'Telangana'
  },
  feesPaid: true
}
{
  _id: ObjectId('682d54269101f6a1cfbe9434'),
  name: 'Imran Shaikh',
  studentId: 104,
  courses: [
    'Data Science',
    'Java'
  ],
  address: {
    city: 'Delhi',
    state: 'Delhi'
  },
  feesPaid: false
}

4. Find all students enrolled in "Python" 

db.enrollments.find({ courses: "Python" })

Output:

{
  _id: ObjectId('682d54269101f6a1cfbe9431'),
  name: 'Ananya Verma',
  studentId: 101,
  courses: [
    'Python',
    'Java'
  ],
  address: {
    city: 'Delhi',
    state: 'Delhi'
  },
  feesPaid: true
}
{
  _id: ObjectId('682d54269101f6a1cfbe9432'),
  name: 'Rohan Mehta',
  studentId: 102,
  courses: [
    'Python',
    'AI'
  ],
  address: {
    city: 'Bangalore',
    state: 'Karnataka'
  },
  feesPaid: false
}

5. Find students from Delhi who have not paid fees 

db.enrollments.find({ 
  "address.city": "Delhi", 
  feesPaid: false 
})

Output:

{
  _id: ObjectId('682d54269101f6a1cfbe9434'),
  name: 'Imran Shaikh',
  studentId: 104,
  courses: [
    'Data Science',
    'Java'
  ],
  address: {
    city: 'Delhi',
    state: 'Delhi'
  },
  feesPaid: false
}

6. Add a new course "AI Fundamentals" to a specific student's courses array 

db.enrollments.updateOne(
  { studentId: 101 },
  { $addToSet: { courses: "AI Fundamentals" } }
)

Output:

{
  acknowledged: true,
  insertedId: null,
  matchedCount: 1,
  modifiedCount: 1,
  upsertedCount: 0
}

7. Update the city of a specific student to "Mumbai" 

db.enrollments.updateOne(
  { studentId: 102 },
  { $set: { "address.city": "Mumbai" } }
)

Output:

{
  acknowledged: true,
  insertedId: null,
  matchedCount: 1,
  modifiedCount: 1,
  upsertedCount: 0
}

8. Set feesPaid to true for all students from "Delhi"

db.enrollments.updateMany(
  { "address.city": "Delhi" },
  { $set: { feesPaid: true } }
)
 Output:

{
  acknowledged: true,
  insertedId: null,
  matchedCount: 2,
  modifiedCount: 1,
  upsertedCount: 0
}

9. Remove "Java" course from any student who has it 

db.enrollments.updateMany(
  { courses: "Java" },
  { $pull: { courses: "Java" } }
)

Output:

{
  acknowledged: true,
  insertedId: null,
  matchedCount: 2,
  modifiedCount: 2,
  upsertedCount: 0
}

10.Delete all students who have no courses enrolled (i.e., courses: [] ) 

db.enrollments.deleteMany({ courses: { $size: 0 } })

Output:

{
  acknowledged: true,
  deletedCount: 1
}

