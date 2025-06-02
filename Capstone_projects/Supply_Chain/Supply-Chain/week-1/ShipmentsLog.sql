test> use ShipmentsLog
switched to db ShipmentsLog
ShipmentsLog> db.shipments.insertMany([
... {
...     "shipment_id": "SHP003",
...     "order_id": 3,
...     "shipped_date": "2025-06-01T15:30:00Z",
...     "delivery_date": "2025-06-05T16:45:00Z",
...     "status": "Delivered",
...     "carrier": "BlueDart"
...   },
...   {
...     "shipment_id": "SHP004",
...     "order_id": 4,
...     "shipped_date": "2025-06-03T09:00:00Z",
...     "delivery_date": "2025-06-09T18:00:00Z",
...     "status": "Delivered",
...     "carrier": "DHL"
...   },
...   {
...     "shipment_id": "SHP005",
...     "order_id": 5,
...     "shipped_date": "2025-06-04T08:00:00Z",
...     "delivery_date": null,
...     "status": "In Transit",
...     "carrier": "FedEx"
...   },
...   {
...     "shipment_id": "SHP006",
...     "order_id": 6,
...     "shipped_date": "2025-06-05T10:30:00Z",
...     "delivery_date": null,
...     "status": "In Transit",
...     "carrier": "BlueDart"
...   },
...   {
...     "shipment_id": "SHP007",
...     "order_id": 7,
...     "shipped_date": "2025-06-06T11:45:00Z",
...     "delivery_date": "2025-06-12T13:15:00Z",
...     "status": "Delivered",
...     "carrier": "Delhivery"
...   },
...   {
...     "shipment_id": "SHP008",
...     "order_id": 8,
...     "shipped_date": "2025-06-06T14:30:00Z",
...     "delivery_date": null,
...     "status": "Pending",
...     "carrier": "Speedy"
...   },
...   {
...     "shipment_id": "SHP009",
...     "order_id": 9,
...     "shipped_date": "2025-06-07T08:15:00Z",
...     "delivery_date": "2025-06-11T10:00:00Z",
...     "status": "Delivered",
...     "carrier": "FastShip"
...   },
...   {
...     "shipment_id": "SHP010",
...     "order_id": 10,
...     "shipped_date": "2025-06-07T09:00:00Z",
...     "delivery_date": null,
...     "status": "In Transit",
...     "carrier": "DHL"
...   }])
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId("683d6b26622a57b4ec3801b6"),
    '1': ObjectId("683d6b26622a57b4ec3801b7"),
    '2': ObjectId("683d6b26622a57b4ec3801b8"),
    '3': ObjectId("683d6b26622a57b4ec3801b9"),
    '4': ObjectId("683d6b26622a57b4ec3801ba"),
    '5': ObjectId("683d6b26622a57b4ec3801bb"),
    '6': ObjectId("683d6b26622a57b4ec3801bc"),
    '7': ObjectId("683d6b26622a57b4ec3801bd")
  }
}

