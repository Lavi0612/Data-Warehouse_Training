class Vehicle:
    def __init__(self, name, wheels):
        self.name = name
        self.wheels = wheels

    def description(self):
        return f"{self.name} with {self.wheels} wheels"

class Car(Vehicle):
    def __init__(self, name, wheels, fuel_type):
        super().__init__(name, wheels)
        self.fuel_type = fuel_type

    def description(self):
        return f"Car: {self.name}, Fuel: {self.fuel_type}, Wheels: {self.wheels}"

class Bike(Vehicle):
    def __init__(self, name, wheels, is_geared):
        super().__init__(name, wheels)
        self.is_geared = is_geared

    def description(self):
        return f"Bike: {self.name}, {'Geared' if self.is_geared else 'Non-Geared'}, Wheels: {self.wheels}"

# Car
car_name = input("Enter car name: ")
car_fuel = input("Fuel type: ")
car = Car(car_name, 4, car_fuel)
print(car.description())

# Bike
bike_name = input("Enter bike name: ")
is_geared = input("Is the bike geared? (yes/no): ").lower() == 'yes'
bike = Bike(bike_name, 2, is_geared)
print(bike.description())
print()