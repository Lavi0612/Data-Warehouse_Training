use customer_feedback
switched to db customer_feedback

--Feedback Insertions 
db.feedback.insertMany([
  { customer_id: 1, feedback: "Delivery was delayed by 2 days, but support was responsive.", submitted_at: ISODate("2024-06-08T09:00:00Z") },
  { customer_id: 2, feedback: "Received on time, very satisfied with service.", submitted_at: ISODate("2024-06-07T10:30:00Z") },
  { customer_id: 3, feedback: "Package arrived late and was slightly damaged.", submitted_at: ISODate("2024-06-09T15:20:00Z") },
  { customer_id: 4, feedback: "Fast delivery, product matches description.", submitted_at: ISODate("2024-06-10T11:45:00Z") },
  { customer_id: 5, feedback: "Delivery took longer than expected, but overall happy.", submitted_at: ISODate("2024-06-11T12:00:00Z") },
  { customer_id: 6, feedback: "Arrived on time, great packaging!", submitted_at: ISODate("2024-06-12T14:10:00Z") },
  { customer_id: 7, feedback: "Delayed shipment caused inconvenience.", submitted_at: ISODate("2024-06-13T08:00:00Z") },
  { customer_id: 8, feedback: "Excellent service, on-time delivery.", submitted_at: ISODate("2024-06-14T09:15:00Z") },
  { customer_id: 9, feedback: "Late delivery, but quality product.", submitted_at: ISODate("2024-06-15T16:45:00Z") },
  { customer_id: 10, feedback: "Received early and well packaged.", submitted_at: ISODate("2024-06-16T10:30:00Z") },
  { customer_id: 11, feedback: "Delivery was delayed, customer support was helpful.", submitted_at: ISODate("2024-06-17T12:00:00Z") },
  { customer_id: 12, feedback: "Perfect experience, no delays.", submitted_at: ISODate("2024-06-18T11:20:00Z") },
  { customer_id: 13, feedback: "Product arrived late, but satisfied overall.", submitted_at: ISODate("2024-06-19T13:35:00Z") },
  { customer_id: 14, feedback: "On-time delivery, would buy again.", submitted_at: ISODate("2024-06-20T15:00:00Z") },
  { customer_id: 15, feedback: "Shipping delay was frustrating.", submitted_at: ISODate("2024-06-21T09:50:00Z") },
  { customer_id: 16, feedback: "Delivery arrived as scheduled, thanks!", submitted_at: ISODate("2024-06-22T10:00:00Z") },
  { customer_id: 17, feedback: "Late delivery but quality product.", submitted_at: ISODate("2024-06-23T14:15:00Z") },
  { customer_id: 18, feedback: "Very fast delivery, highly recommend.", submitted_at: ISODate("2024-06-24T11:25:00Z") },
  { customer_id: 19, feedback: "Order arrived late and without tracking updates.", submitted_at: ISODate("2024-06-25T10:10:00Z") },
  { customer_id: 20, feedback: "Excellent, no delays.", submitted_at: ISODate("2024-06-26T09:45:00Z") },
  { customer_id: 21, feedback: "Delayed but good customer service.", submitted_at: ISODate("2024-06-27T08:30:00Z") },
  { customer_id: 22, feedback: "On-time delivery, product as expected.", submitted_at: ISODate("2024-06-28T10:40:00Z") },
  { customer_id: 23, feedback: "Delay caused issues, but order arrived.", submitted_at: ISODate("2024-06-29T12:00:00Z") },
  { customer_id: 24, feedback: "Great service, timely delivery.", submitted_at: ISODate("2024-06-30T14:20:00Z") },
  { customer_id: 25, feedback: "Late delivery but satisfied with product.", submitted_at: ISODate("2024-07-01T09:15:00Z") },
  { customer_id: 26, feedback: "Delivery on time, happy customer!", submitted_at: ISODate("2024-07-02T10:50:00Z") },
  { customer_id: 27, feedback: "Shipment delayed, poor communication.", submitted_at: ISODate("2024-07-03T11:10:00Z") },
  { customer_id: 28, feedback: "On-time delivery and excellent quality.", submitted_at: ISODate("2024-07-04T12:30:00Z") },
  { customer_id: 29, feedback: "Late delivery but product met expectations.", submitted_at: ISODate("2024-07-05T14:45:00Z") },
  { customer_id: 30, feedback: "Received early, very happy!", submitted_at: ISODate("2024-07-06T09:55:00Z") }
])


--Query Feedback by Customer ID
db.feedback.createIndex({ customer_id: 1 })

db.feedback.find({ customer_id: 3 }).pretty()

--Text Search on Feedback Content
db.feedback.createIndex({ feedback: "text" })

db.feedback.find({ $text: { $search: "delivery" } })
