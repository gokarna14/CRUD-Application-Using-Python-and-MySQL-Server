import Objects


Objects.Customer.createSingleRecord(
    name="Gokarna Adpssjhkjbnhikari", 
    email='pxxxga@gmail.com', 
    phone="55-5550003", 
    address='Banasthali')



Objects.Customer.deleteRecord(
    name="Gokarna Adhikari", 
    email='pga@gmail.com', 
    phone="55-555003", 
    address='Banasthali')

Objects.Customer.readRecord(
    selectColumns=['NAME'],
    name='Gokarna Adhikari', 
    address='Banasthali')

Objects.Customer.updateRecord(id=1,
    name="Prabas 1", 
    email='pga@gmail.com', 
    phone="55-5550003", 
    address='Banasthali')


