# -*- coding: utf-8 -*-
from soma.pipeline.api import Process, ProcessSignature
from soma.signature.api import Choice, Point3D
from soma.database.api import DataFile

def prepareSubject( somaExecutionContext ):
  import math
  import registration
  from brainvisa import anatomist
  from brainvisa import quaternion

  callerContext = somaExecutionContext.callerContext
  _ = callerContext.getTranslator()
  input = somaExecutionContext.input_parameters

  ac = []
  pc = []
  ip = []
  lh = []
  acmm = input.anterior_commissure
  pcmm = input.posterior_commissure
  ipmm = input.interhemispheric_point
  if normalised == 'No':
    atts = input.t1mri.header
    vs = atts[ 'voxel_size' ]
    ac = input.anterior_commissure
    pc = input.posterior_commissure
    ip = input.interhemispheric_point
    lh = input.left_hemisphere_point
    if not ac or len( ac ) != 3 or not pc or len( pc ) != 3 or not ip or \
      len(ip ) != 3:
      raise RuntimeError( _( 'In non-normalized mode, the 3 points AC, PC '
	'and IP are mandatory (in mm)' ) )

    def vecproduct( v1, v2 ):
      return ( v1[1] * v2[2] - v1[2] * v2[1],
	  v1[2] * v2[0] - v1[0] * v2[2],
	  v1[0] * v2[1] - v1[1] * v2[0] )
    def dot( v1, v2 ):
      return v1[0] * v2[0] + v1[1] * v2[1] + v1[2] * v2[2]
    def norm( v ):
      return math.sqrt( v[0] * v[0] + v[1] * v[1] + v[2] * v[2] )
    def normalize( v ):
      n = 1. / norm(v)
      return ( v[0] * n, v[1] * n, v[2] * n )
    def vecscale( v, scl ):
      return ( v[0] * scl, v[1] * scl, v[2] * scl )

    # determine image orientation
    v1 = normalize( ( pc[0] - ac[0], pc[1] - ac[1], pc[2] - ac[2] ) )
    v2 = normalize( ( ac[0] - ip[0], ac[1] - ip[1], ac[2] - ip[2] ) )
    v3 = normalize( vecproduct( v1, v2 ) )
    v2 = normalize( vecproduct( v3, v1 ) )

    # determine rotation between y axis and v1
    y = ( 0, 1, 0 )
    n = vecproduct( y, v1 )
    cosal = dot( y, v1 )
    if norm( n ) < 1e-5:
      if cosal > 0:
	r1 = quaternion.Quaternion( ( 0, 0, 0, 1 ) )
      else:
	r1 = quaternion.Quaternion( ( 0, 0, 1, 0 ) ) # flip z
    else:
      n = normalize( n )
      t = vecproduct( y, n )
      alpha = math.acos( cosal )
      if dot( t, v1 ) < 0:
	alpha = -alpha
      r1 = quaternion.Quaternion()
      r1.fromAxis( n, alpha )

    # apply r1 to ( v1, v2, v3 )
    v1_1 = r1.transform( v1 )
    v2_1 = r1.transform( v2 )
    v3_1 = r1.transform( v3 )
    # now v1_1 should be aligned on y

    # determine rotation between z axis and v2
    z = ( 0, 0, 1 )
    p = dot( z, v2_1 )
    alpha = math.acos( p )
    q = dot( normalize( vecproduct( z, v2_1 ) ), y )
    if q >= 0:
      alpha = -alpha
    r2 = quaternion.Quaternion()
    r2.fromAxis( y, alpha )

    # apply r2 to ( v1, v2, v3 )
    v1_2 = r2.transform( v1_1 )
    v2_2 = r2.transform( v2_1 )
    v3_2 = r2.transform( v3_1 )

    r3 = r1.compose( r2 )
    trans = r3.rotationMatrix()

    # check x inversion
    if lh is None:
      callerContext.warning( _( 'Left hemisphere point not specified - X axis ' \
	'flip will not be checked' ) )
    else:
      x = ( 1, 0, 0 )
      lvec = r3.transformInverse( ( lh[0] - ac[0], lh[1] - ac[2], lh[2] - ac[2] ) )
      if dot( x, lvec ) < 0:
	context.write( _t_( 'X is flipped' ) )
	trans[0] *= -1
	trans[1] *= -1
	trans[2] *= -1
	trans[3] *= -1

    # build binary flip matrix
    flipmat = [ 0, 0, 0, 0,  0, 0, 0, 0,  0, 0, 0, 0,  0, 0, 0, 1 ]
    dims = atts[ 'volume_dimension' ][:3]
    dims2 = ( dims[0] * vs[0], dims[1] * vs[1], dims[2] * vs[2] )
    imax = 0
    for i in range(1,3):
      if abs( trans[i] ) > abs( trans[imax] ):
	imax = i
    if trans[imax] >= 0:
      flipmat[imax] = 1
    else:
      flipmat[imax] = -1
      flipmat[3] = dims2[imax]
    imax = 4
    for i in range(5,7):
      if abs( trans[i] ) > abs( trans[imax] ):
	imax = i
    if trans[imax] >= 0:
      flipmat[imax] = 1
    else:
      flipmat[imax] = -1
      flipmat[7] = dims2[imax%4]
    imax = 8
    for i in range(9,11):
      if abs( trans[i] ) > abs( trans[imax] ):
	imax = i
    if trans[imax] >= 0:
      flipmat[imax] = 1
    else:
      flipmat[imax] = -1
      flipmat[11] = dims2[imax%4]
    needsflip = False
    if flipmat != [ 1, 0, 0, 0,  0, 1, 0, 0,  0, 0, 1, 0,  0, 0, 0, 1 ]:
      callerContext.warning( _( 'Flip needed, with matrix:' ) )
      callerContext.write( '[', flipmat[0:4] )
      callerContext.write( ' ', flipmat[4:8] )
      callerContext.write( ' ', flipmat[8:12] )
      callerContext.write( ' ', flipmat[12:16], ']' )
      needsflip = True
    else:
      callerContext.write( '<font color="#00c000">OK, the image seems to be in the correct orientation.</font>' )

    if needsflip and not input.allow_flip_initial_MRI:
      callerContext.write( '<b>Image needs to be flipped</b>, but you did not ' \
		      'allow it, so it won\'t change. Expect problems in ' \
		      'hemispheres separation, and sulci recognition will ' \
		      'not work anyway.' )
    if needsflip and input.allow_flip_initial_MRI:
      def matrixMult( m, p ):
	return [ m[0] * p[0] + m[1] * p[1] + m[2] * p[2] + m[3],
		  m[4] * p[0] + m[5] * p[1] + m[6] * p[2] + m[7],
		  m[8] * p[0] + m[9] * p[1] + m[10] * p[2] + m[11] ]

      fliprot = flipmat[:3] + [ 0 ] + flipmat[4:7] + [0] + flipmat[8:11] \
	+ [0, 0, 0, 0, 1 ]
      vs2 = map( abs, matrixMult( fliprot, vs ) )
      dims = atts[ 'volume_dimension' ][:3]
      dims2 = ( dims[0] * vs[0], dims[1] * vs[1], dims[2] * vs[2] )
      dims3 = matrixMult( fliprot, dims2 )
      dims4 = map( lambda x,y: int( round( abs( x ) / y ) ), dims3, vs2 )

      #flip[9] = -min( 0, dims3[0] )
      #flip[10] = -min( 0, dims3[1] )
      #flip[11] = -min( 0, dims3[2] )

      callerContext.write( '<b><font color="#c00000">WARNING:</font> Flipping and ' \
		      're-writing source image</b>' )
      callerContext.write( 'voxel size orig :', vs )
      callerContext.write( 'voxel size final:', vs2 )
      callerContext.write( 'dims orig :', dims )
      callerContext.write( 'dims final:', dims4 )
      callerContext.write( 'transformation:', flipmat )

      mfile = somaExecutionContext.temporaryDataFile( 'Transformation matrix' )
      mf = open( mfile.fullPath, 'w' )
      mf.write( string.join( map( str, flipmat[3:12:4] ) ) + '\n' )
      mf.write( string.join( map( str, flipmat[:3] ) ) + '\n' )
      mf.write( string.join( map( str, flipmat[4:7] ) ) + '\n' )
      mf.write( string.join( map( str, flipmat[8:11] ) ) + '\n' )
      mf.close()
      callerContext.log( 'Transformation',
		    html = 'transformation: R = ' + str( flipmat[:3] \
		    + flipmat[4:7] + flipmat[8:11] ) \
		    + ', T = ' + str( flipmat[3:12:4] ) )

      somaExecutionContext.system( 'AimsResample', '-i', input.t1mri.fullPath,
		      '-o', output.t1mri.fullPath, '-m', mfile.fullPath,
		      '--sx', vs2[0], '--sy', vs2[1], '--sz', vs2[2],
		      '--dx', dims4[0], '--dy', dims4[1], '--dz', dims4[2] )

      acmm = matrixMult( flipmat, ac )
      pcmm = matrixMult( flipmat, pc )
      ipmm = matrixMult( flipmat, ip )
      callerContext.write( 'new AC:', acmm )
      callerContext.write( 'new PC:', pcmm )
      callerContext.write( 'new IP:', ipmm )
      vs = vs2

    ac = [ int( acmm[0] / vs[0] +0.5 ), int( acmm[1] / vs[1] +0.5),
	    int( acmm[2] / vs[2] +0.5) ]
    pc = [ int( pcmm[0] / vs[0] +0.5 ), int( pcmm[1] / vs[1] +0.5),
	    int( pcmm[2] / vs[2] +0.5) ]
    ip = [ int( ipmm[0] / vs[0] +0.5 ), int( ipmm[1] / vs[1] +0.5),
	    int( ipmm[2] / vs[2] +0.5) ]

  # normalized case
  else:
    atts = input.t1mri.header
    refs = atts.get( 'referentials' )
    trans = atts.get( 'transformations' )
    vs = atts[ 'voxel_size' ]
    autonorm = False
    if refs and trans:
      for i in range( len( refs ) ):
	if refs[i] == 'Talairach-MNI template-SPM':
	  break
      if i >= len( refs ):
	i = 0
      tr = trans[0]
      try:
	import soma.aims as aims
	a2t = aims.Motion( tr )
	t2a = a2t.inverse()
	acmm = t2a.transform( [ 0, 0, 0 ] )
	ac = [ int( acmm[0] / vs[0] ), int( acmm[1] / vs[1] ),
		int( acmm[2] / vs[2] ) ]
	pcmm = t2a.transform( [ 0, -28, 0 ] )
	pc = [ int( pcmm[0] / vs[0] ), int( pcmm[1] / vs[1] ),
		int( pcmm[2] / vs[2] ) ]
	ipmm = t2a.transform( [ 0, -20, 60 ] )
	ip = [ int( ipmm[0] / vs[0] ), int( ipmm[1] / vs[1] ),
		int( ipmm[2] / vs[2] ) ]
	autonorm = True
      except Exception, e:
	callerContext.warning( e )

    if not autonorm:
      if input.normalised == 'MNI from SPM' :
	ac = [ 77, 73, 88 ]
	pc = [ 77, 100, 83 ]
	ip = [ 76, 60, 35 ]
	acmm = ac
	pcmm = pc
	ipmm = ip
	#self.T1mri.setMinf( 'referential',
			    #registration.talairachMNIReferentialId )
	#try:
	  #self.T1mri.saveMinf()
	#except:
	  #context.warning( 'could not set SPM/MNI normalized '
			  #'referential to', self.T1mri.fullName() )
      elif input.normalised == 'MNI from Mritotal' :
	ac = [ 91, 88, 113 ]
	pc = [ 91, 115, 109 ]
	ip = [ 90, 109, 53 ]
	acmm = ac
	pcmm = pc
	ipmm = ip
      elif input.normalised == 'Marseille from SPM' :
	ac = [ 91, 93, 108 ]
	pc = [ 91, 118, 106 ]
	ip = [ 91, 98, 68 ]
	acmm = ac
	pcmm = pc
	ipmm = ip

  f = open( output.commissure_coordinates.fullPath, 'w' )
  f.write( "AC: " + string.join( map( lambda x:str(x), ac ) ) + '\n' )
  f.write( "PC: " + string.join( map( lambda x:str(x), pc ) ) + '\n' )
  f.write( "IH: " + string.join( map( lambda x:str(x), ip ) ) + '\n' )
  f.write( "The previous coordinates, used by the system, are defined in " \
	    "voxels\n" )
  f.write( "They stem from the following coordinates in millimeters:\n" )
  if input.normalised == 'No':
    f.write( "ACmm: " + string.join( map( str, acmm ) ) + '\n' )
    f.write( "PCmm: " + string.join( map( str, pcmm ) ) + '\n' )
    f.write( "IHmm: " + string.join( map( str, ipmm ) ) + '\n' )
  else:
    f.write( "ACmm: " + string.join( map( str, ac ) ) + '\n' )
    f.write( "PCmm: " + string.join( map( str, pc ) ) + '\n' )
    f.write( "IHmm: " + string.join( map( str, ip ) ) + '\n' )
  f.close()

  # manage referential
  tm = somaExecutionContext.getTransformationManager()
  ref = tm.referential( output.t1mri )
  if ref is None:
    tm.createNewReferentialFor( output.t1mri )


from soma.pipeline.api import point3DLink
from soma.database.api import databaseLink


class APCReader:
  def __init__( self, key ):
    self._key = key + 'mm:'
    
  def __call__( self, values, process ):
    acp = None
    if values.Commissure_coordinates is not None:
      acp = values.Commissure_coordinates
    #elif values.T1mri:
    #  acp = ReadDiskItem( 'Commissure coordinates','Commissure coordinates')\
    #    .findValue( values.T1mri )
    if acp is not None and acp.isReadable():
      f = open( acp.fullPath )
      for l in f.readlines():
        if l[ :len(self._key) ] == self._key:
          return map( float, string.split( l[ len(self._key)+1: ] ) )


def linknorm( values, process ):
  if values.T1mri and values.T1mri.get( 'normalized' ) == 'yes':
    return 'MNI from SPM'
  return 'No'


class PrepareSubject( Process ):
  name = 'Prepare Subject for Anatomical Pipeline'
  userLevel = 0
  # Version of the process
  version = '4.0.0'

  # Parameters that are for input only
  signature = ProcessSignature( 
    't1mri', DataFile( 'Raw T1 MRI', 'Aims readable volume formats' ), dict( input=True, output=True ),
    'is_normalised', Choice( 'No', 'MNI from SPM', 'MNI from Mritotal', 'Marseille from SPM' ), dict( defaultValue='No' ),
    'anterior_commissure', Point3D(), dict( optional=True ),
    'posterior_commissure', Point3D(), dict( optional=True ), 
    'inter_hemispheric_point', Point3D(), dict( optional=True ),
    'left_Hemisphere_point', Point3D(), dict( optional=True ),
    'allow_flip_initial_MRI', Boolean(),  dict( defaultValue=False ),
    'commissure_coordinates', DataFile( 'Commissure coordinates','Commissure coordinates'), dict( output=True ),
  )

  # Dependencies are a way to detect changes in the target system that
  # could modify the behaviour of the process.
  dependencies = 'soma-base, aims-free'

  # The execution function is not a method anymore because there is no instanciation of a Process
  # when the process is executed. Distant execution of the process excution function could be done
  # either through pickle or by using two attributes, __module__ and __name__, with the following
  # code:
  #   code = 'from ' + exeution.__module__ + ' import ' + execution.__name__ + ' as f\nf(' + args + ')'
  #   exec( code )
  execution = prepareSubject  
  
  def __init__( self ):
    self.addParameterLink( DatabaseLink, 'commissure_coordinates', 't1mri' )
    self.addParameterLink( Point3DLink, 'anterior_commisure', 't1mri' )
    self.addParameterLink( Point3DLink, 'posterior_commisure', 't1mri' )
    self.addParameterLink( Point3DLink, 'interhemispheric_commisure', 't1mri' )
    self.addParameterLink( Point3DLink, 'left_hemisphere_point', 't1mri' )
    self.addParameterLink( APCReader( 'AC' ), 'anterior_commisure', 'commissure_coordinates' )
    self.addParameterLink( APCReader( 'PC' ), 'posterior_commisure', 'commissure_coordinates' )
    self.addParameterLink( APCReader( 'IH' ), 'interhemispheric_point', 'commissure_coordinates' )
    self.addParameterLink( linknorm, 'normalised', 't1mri' )

