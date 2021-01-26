# TASK 1.3
**This readme is a small documentation for our task.**

As a team ([Prekas](https://github.com/PrekWood), [Brinias](https://github.com/IliasBrinias)) we create:
* Hash Faction
* Hash Insert
* Hash Find
* Hash Delete
* Hash Join
* Hash Drop
* Hash Visualization


## Basic idea 

### Hash Idexing 

The Hash Index is a **list that every cell is a Bucket object**.
The bucket object has the attribute **data** which is a list with format:

`data = [[value0 ,pointer0_to_DB],[value1 ,pointer1_to_DB]]`

### Hash Faction

The hash faction that we used is the `VALUE mod (2**i)`,allocate the records based on
value of `i`-less significant bits (i-LSB)

## How does it work?

You can **create** the HashIndex with this command:
```python
>> db_object.create_index(table_name, index_name, column_name, index_type = 'HashIndex')
```

You can **create** the HashIndex when you create a table:
```python
>> db_object.create_table(name, column_names, column_types, primary_key, load, create_hashindex=True)
```
Every column of the table can have Hash Index but **can handle only "==" conditions**.

When you run a **select** command the code automatically decides the best situation for you. 
Also, the Hash Join command checks which table have Hash Indexes and executes the best situation.

Every time you Insert or Delete elements from a table, the hash index updates automatically.
If you decide to drop the table, all the table's indexes will delete automatically.

You can see the buckets with the visualization tool graphviz with the command:
```python
>> db_object.show_hashindex(index_name)
```