Collection: students 
Insertion:

db.students.insertMany([
  {
    name: "Ayaan Khan",
    age: 21,
    gender: "Male",
    department: "Computer Science",
    courses: [
      { name: "MongoDB", score: 85 },
      { name: "Python", score: 90 }
    ],
    address: {
      city: "Hyderabad",
      state: "Telangana",
      pincode: 500032
    },
    enrollmentDate: new Date("2024-08-01"),
    isActive: true
  },
  {
    name: "John Doe",
    age: 22,
    gender: "Male",
    department: "Mechanical",
    courses: [
      { name: "C++", score: 70 },
      { name: "Python", score: 60 }
    ],
    address: {
      city: "Mumbai",
      state: "Maharashtra",
      pincode: 400001
    },
    enrollmentDate: new Date("2023-07-15"),
    isActive: false
  },
  {
    name: "Sana Malik",
    age: 20,
    gender: "Female",
    department: "Mathematics",
    courses: [
      { name: "R", score: 95 },
      { name: "Python", score: 88 }
    ],
    address: {
      city: "Delhi",
      state: "Delhi",
      pincode: 110001
    },
    enrollmentDate: new Date("2024-02-01"),
    isActive: true
  },
  {
    name: "Rohan Mehta",
    age: 23,
    gender: "Male",
    department: "Computer Science",
    courses: [
      { name: "Node.js", score: 92 },
      { name: "MongoDB", score: 87 }
    ],
    address: {
      city: "Ahmedabad",
      state: "Gujarat",
      pincode: 380001
    },
    enrollmentDate: new Date("2023-10-10"),
    isActive: true
  },
  {
    name: "Priya Sharma",
    age: 19,
    gender: "Female",
    department: "Mechanical",
    courses: [
      { name: "Thermodynamics", score: 78 },
      { name: "AutoCAD", score: 82 }
    ],
    address: {
      city: "Chandigarh",
      state: "Punjab",
      pincode: 160017
    },
    enrollmentDate: new Date("2025-01-12"),
    isActive: false
  },
  {
    name: "Neha Verma",
    age: 22,
    gender: "Female",
    department: "Mathematics",
    courses: [
      { name: "Statistics", score: 91 },
      { name: "Python", score: 85 }
    ],
    address: {
      city: "Lucknow",
      state: "Uttar Pradesh",
      pincode: 226001
    },
    enrollmentDate: new Date("2024-06-20"),
    isActive: true
  },
  {
    name: "Karan Patel",
    age: 25,
    gender: "Male",
    department: "Computer Science",
    courses: [
      { name: "C++", score: 80 },
      { name: "Python", score: 70 }
    ],
    address: {
      city: "Vadodara",
      state: "Gujarat",
      pincode: 390001
    },
    enrollmentDate: new Date("2024-09-01"),
    isActive: false
  },
  {
    name: "Meera Iyer",
    age: 24,
    gender: "Female",
    department: "Mathematics",
    courses: [
      { name: "Mathematica", score: 88 },
      { name: "R", score: 92 }
    ],
    address: {
      city: "Chennai",
      state: "Tamil Nadu",
      pincode: 600001
    },
    enrollmentDate: new Date("2023-12-12"),
    isActive: true
  },
  {
    name: "Farhan Sheikh",
    age: 23,
    gender: "Male",
    department: "Mechanical",
    courses: [
      { name: "AutoCAD", score: 79 },
      { name: "SolidWorks", score: 84 }
    ],
    address: {
      city: "Nagpur",
      state: "Maharashtra",
      pincode: 440001
    },
    enrollmentDate: new Date("2024-03-01"),
    isActive: false
  },
  {
    name: "Aarav Jain",
    age: 21,
    gender: "Male",
    department: "Computer Science",
    courses: [
      { name: "MongoDB", score: 81 },
      { name: "Python", score: 89 }
    ],
    address: {
      city: "Hyderabad",
      state: "Telangana",
      pincode: 500034
    },
    enrollmentDate: new Date("2023-09-25"),
    isActive: true
  },
  {
    name: "Isha Rao",
    age: 20,
    gender: "Female",
    department: "Computer Science",
    courses: [
      { name: "Python", score: 92 },
      { name: "Node.js", score: 89 }
    ],
    address: {
      city: "Bangalore",
      state: "Karnataka",
      pincode: 560001
    },
    enrollmentDate: new Date("2024-05-01"),
    isActive: true
  },
  {
    name: "Dev Singh",
    age: 22,
    gender: "Male",
    department: "Mathematics",
    courses: [
      { name: "Python", score: 84 },
      { name: "R", score: 78 }
    ],
    address: {
      city: "Jaipur",
      state: "Rajasthan",
      pincode: 302001
    },
    enrollmentDate: new Date("2024-04-10"),
    isActive: false
  },
  {
    name: "Ananya Das",
    age: 23,
    gender: "Female",
    department: "Computer Science",
    courses: [
      { name: "Python", score: 95 },
      { name: "Node.js", score: 93 }
    ],
    address: {
      city: "Kolkata",
      state: "West Bengal",
      pincode: 700001
    },
    enrollmentDate: new Date("2023-11-01"),
    isActive: true
  },
  {
    name: "Zara Ali",
    age: 24,
    gender: "Female",
    department: "Mechanical",
    courses: [
      { name: "SolidWorks", score: 90 },
      { name: "Thermodynamics", score: 86 }
    ],
    address: {
      city: "Indore",
      state: "Madhya Pradesh",
      pincode: 452001
    },
    enrollmentDate: new Date("2024-01-20"),
    isActive: false
  },
  {
    name: "Ali Hussain",
    age: 26,
    gender: "Male",
    department: "Computer Science",
    courses: [
      { name: "MongoDB", score: 92 },
      { name: "Python", score: 86 }
    ],
    address: {
      city: "Hyderabad",
      state: "Telangana",
      pincode: 500033
    },
    enrollmentDate: new Date("2024-08-10"),
    isActive: true
  },
  {
    name: "Simran Kaur",
    age: 20,
    gender: "Female",
    department: "Mathematics",
    courses: [
      { name: "Statistics", score: 88 },
      { name: "Python", score: 75 }
    ],
    address: {
      city: "Amritsar",
      state: "Punjab",
      pincode: 143001
    },
    enrollmentDate: new Date("2023-06-01"),
    isActive: true
  },
  {
    name: "Nikhil Rao",
    age: 27,
    gender: "Male",
    department: "Mechanical",
    courses: [
      { name: "AutoCAD", score: 85 },
      { name: "Thermodynamics", score: 88 }
    ],
    address: {
      city: "Pune",
      state: "Maharashtra",
      pincode: 411001
    },
    enrollmentDate: new Date("2024-07-01"),
    isActive: false
  },
  {
    name: "Ritika Mishra",
    age: 21,
    gender: "Female",
    department: "Computer Science",
    courses: [
      { name: "Python", score: 83 },
      { name: "MongoDB", score: 78 }
    ],
    address: {
      city: "Bhopal",
      state: "Madhya Pradesh",
      pincode: 462001
    },
    enrollmentDate: new Date("2024-03-15"),
    isActive: true
  },
  {
    name: "Aman Gupta",
    age: 22,
    gender: "Male",
    department: "Computer Science",
    courses: [
      { name: "Node.js", score: 84 },
      { name: "Python", score: 81 }
    ],
    address: {
      city: "Noida",
      state: "Uttar Pradesh",
      pincode: 201301
    },
    enrollmentDate: new Date("2024-04-25"),
    isActive: true
  }
]);

CRUD Operations 
  
1. Insert a new student record with embedded courses and address data. 

db.students.insertOne({
  name: "Tanya Singh",
  age: 19,
  gender: "Female",
  department: "Computer Science",
  courses: [
    { name: "Python", score: 88 },
    { name: "Node.js", score: 90 }
  ],
  address: {
    city: "Dehradun",
    state: "Uttarakhand",
    pincode: 248001
  },
  enrollmentDate: ISODate("2024-08-15T00:00:00Z"),
  isActive: true
});

2. Update score for a course ( Python ) inside the courses array. 

db.students.updateOne(
  { name: "Ayaan Khan", "courses.name": "Python" },
  { $set: { "courses.$.score": 95 } }
);

3. Delete a student whose name is "John Doe" . 

db.students.deleteOne({ name: "John Doe" });

4. Find all students in the "Computer Science" department. 

db.students.find({ department: "Computer Science" };

5. Find students where age is greater than 20. 

db.students.find({ age: { $gt: 20 } });

6. Find students enrolled between two dates. 

db.students.find({
  enrollmentDate: {
    $gte: ISODate("2024-01-01T00:00:00Z"),
    $lte: ISODate("2024-12-31T00:00:00Z")
  }
});

7. Find students who are either in "Computer Science" or "Mathematics". 

db.students.find({
  department: { $in: ["Computer Science", "Mathematics"] }
});

8. Find students not in the "Mechanical" department. 

db.students.find({
  department: { $ne: "Mechanical" }
});

9. Find students whose courses.score is greater than 80. 

db.students.find({
  "courses.score": { $gt: 80 }
});

Aggregation Framework 
  
10. Group by department and count students. 

db.students.aggregate([
  { $group: { _id: "$department", total: { $sum: 1 } } }
]);

11. Calculate average age of students per department. 

db.students.aggregate([
  { $group: { _id: "$department", avgAge: { $avg: "$age" } } }
]);

12. Sort students by total course score (computed using $sum inside $project ). 

db.students.aggregate([
  {
    $project: {
      name: 1,
      totalScore: { $sum: "$courses.score" }
    }
  },
  { $sort: { totalScore: -1 } }
]);

13. Filter only active students before aggregation. 

db.students.aggregate([
  { $match: { isActive: true } },
  {
    $group: {
      _id: "$department",
      count: { $sum: 1 }
    }
  }
]);

14. Group and list unique cities from the address field. 

db.students.aggregate([
  {
    $group: {
      _id: null,
      uniqueCities: { $addToSet: "$address.city" }
    }
  }
]);

Projections 
  
15. Find students with only name , department , and city fields shown. 

db.students.find({}, {
  name: 1,
  department: 1,
  "address.city": 1,
  _id: 0
});

16. Exclude the _id field from output. 

db.students.find({}, { _id: 0 });

17. Show each student's name and total score using $project .

db.students.aggregate([
  {
    $project: {
      name: 1,
      totalScore: { $sum: "$courses.score" }
    }
  }
]);
  
Embedded Documents 
  
18. Query students where address.city = "Hyderabad" .

db.students.find({ "address.city": "Hyderabad" });

19. Update address.pincode for a student. 

db.students.updateOne(
  { name: "Ali Hussain" },
  { $set: { "address.pincode": 500050 } }
);

20. Add a new field landmark to all address objects. 

db.students.updateMany(
  {},
  { $set: { "address.landmark": "Near University" } }
);

Array Operations 
21. Add a new course "Node.js" to a student's courses array. 

db.students.updateOne(
  { name: "Ritika Mishra" },
  { $push: { courses: { name: "Node.js", score: 85 } } }
);

22. Remove a course by name "MongoDB" from the array. 

db.students.updateOne(
  { name: "Ritika Mishra" },
  { $pull: { courses: { name: "MongoDB" } } }
);

23. Find students who have enrolled in both Python and MongoDB. 

db.students.find({
  courses: {
    $all: [
      { $elemMatch: { name: "Python" } },
      { $elemMatch: { name: "MongoDB" } }
    ]
  }
});

24. Use $elemMatch to query students where score in MongoDB > 80.

db.students.find({
  courses: {
    $elemMatch: {
      name: "MongoDB",
      score: { $gt: 80 }
    }
  }
});












