from schema import Schema, String, Float, Int, Double, Bool, List, IntVector, FloatVector, StringVector, Dictionary, Reference


def addDataSchema( schema ):
  schemaName = 'soma.database.data'
  schemaVersion = '0.0.1'
  if schemaName not in schema.schemas:
    schema.schemas[ schemaName ] = schemaVersion
    Entity = schema.Entity

    class Data( Entity ):
      pass
    
    class Image( Data ):
      pass
    
    class Mesh( Data ):
      pass
    