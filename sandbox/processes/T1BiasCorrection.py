# -*- coding: utf-8 -*-
from neuroProcesses import *
import shfjGlobals, registration

def t1_bias_correction( somaExecutionContext ):
  callerContext = somaExecutionContext.callerContext
  _ = callerContext.getTranslator()
  input = somaExecutionContext.input_parameters
  
  if input.mode == 'write_all':
    write_wridges = 'yes'
    write_field = 'yes'
    write_hfiltered = 'yes'
    write_variance = 'yes'
    write_meancurvature = 'yes'
    write_edges = 'yes'
  else:
    write_wridges = input.write_wridges
    write_field = input.write_field
    write_hfiltered = input.write_hfiltered
    write_variance = input.write_variance
    write_meancurvature = input.write_meancurvature
    write_edges = input.write_edges
  if input.edge_mask == 'yes':
    edge = '3'
  else:
    edge = 'n'
  if input.mode in ("write_minimal","write_all"):
    if os.path.exists(self.mri_corrected.fullName() + '.loc'):
      context.write(self.mri_corrected.fullName(), ' has been locked')
      context.write('Remove',self.mri_corrected.fullName(),'.loc if you want to trigger a new correction')
    else:
      somaExecutionContext.system('VipT1BiasCorrection', '-i', input.mri.fullPath, '-o', input.mri_corrected.fullPath , '-Fwrite', input.write_field, '-field', input.field.fullPath, '-Wwrite', self.write_wridges, '-wridge', input.white_ridges.fullPath,'-Kregul', input.field_rigidity, '-sampling',  input.sampling, '-Grid', input.ngrid, '-ZregulTuning', input.zdir_multiply_regul, '-vp',input.variance_fraction,'-e',edge, '-eWrite', input.write_edges, '-ename', input.edges.fullPath(), '-vWrite', input.write_variance, '-vname', input.variance.fullPath, '-mWrite', input.write_meancurvature, '-mname', input.meancurvature.fullPath(), '-hWrite', input.write_hfiltered, '-hname', input.hfiltered.fullPath, '-Last', input.delete_last_n_slices  )
      tm = somaExecutionContext.getTransformationManager()
      tm.copyReferential(input.mri, input.mri_corrected)
      if write_field:
        tm.copyReferential( input.mri, input.field )
      if write_hfiltered:
        tm.copyReferential( input.mri, input.hfiltered )
      if write_wridges:
        tm.copyReferential( input.mri, input.white_ridges )
      if write_variance:
        tm.copyReferential( input.mri, input.variance )
      if write_meancurvature:
        tm.copyReferential( input.mri, input.meancurvature )
      if write_edges:
        tm.copyReferential( input.mri, input.edges )
  elif input.mode == 'delete_useless':
    raise RuntimeError( 'Mode delete_useless is not implemented' )
    # Should be don with database requests
    #if os.path.exists(self.field.fullName() + '.ima') or os.path.exists(self.field.fullName() + '.ima.gz'):
      #shelltools.rm( self.field.fullName() + '.*' )
    #if os.path.exists(self.variance.fullName() + '.ima') or os.path.exists(self.variance.fullName() + '.ima.gz'):
      #shelltools.rm( self.variance.fullName() + '.*' )
    #if os.path.exists(self.edges.fullName() + '.ima') or os.path.exists(self.edges.fullName() + '.ima.gz'):
      #shelltools.rm( self.edges.fullName() + '.*' )
    #if os.path.exists(self.meancurvature.fullName() + '.ima') or os.path.exists(self.meancurvature.fullName() + '.ima.gz'):
      #shelltools.rm( self.meancurvature.fullName() + '.*' )

class T1BiasCorrection( Process ):
  name = 'T1 Bias Correction'
  userLevel = 2

  signature = Signature(
    'mri', DataFile( 'Raw T1 MRI', 'Aims readable volume formats' ),
    'mri_corrected', DataFile( 'T1 MRI Bias Corrected',
         'Aims writable volume formats' ), dict( output=True ),
    'mode', Choice('write_minimal','write_all','delete_useless'), dict( defaultValue='write_minimal' ),
    'write_field', Choice('yes','no'), dict( defaultValue='no' ),
    'field', DataFile( "T1 MRI Bias Field",
       'Aims writable volume formats' ), dict( output=True ),
    'sampling', Float(), dict( defaultValue=16 ),
    'field_rigidity', Float(), dict( defaultValue=20 ),
    'zdir_multiply_regul',Float(), dict( defaultValue=0.5 ),
    'ngrid', Integer(), dict( defaultValue=2 ),
    'write_hfiltered', Choice('yes','no'), dict( defaultValue='yes' ),
    'hfiltered', DataFile( "T1 MRI Filtered For Histo",
        'Aims writable volume formats' ), dict( output=True ),
    'write_wridges', Choice('yes','no','read'), dict( defaultValue='yes' ),
    'white_ridges', DataFile( "T1 MRI White Matter Ridges",
        'Aims writable volume formats' ), dict( output=True ),
    'write_meancurvature', Choice('yes','no'), dict( defaultValue='no' ),
    'meancurvature', DataFile( "T1 MRI Mean Curvature",
        'Aims writable volume formats' ), dict( output=True ),
    'variance_fraction',Integer(), dict( defaultValue=75 ),
    'write_variance', Choice('yes','no'), dict( defaultValue='no' ),
    'variance', DataFile( "T1 MRI Variance",
        'Aims writable volume formats' ), dict( output=True ),
    'edge_mask',Choice('yes','no'), dict( defaultValue='yes' ),
    'write_edges', Choice('yes','no'), dict( defaultValue='no' ),
    'edges', DataFile( "T1 MRI Edges", 'Aims writable volume formats' ), dict( output=True ),
    'delete_last_n_slices',Integer() dict( defaultValue=0 ),
  )

  execution = t1_bias_correction

  def __init__( self ):
    self.addParameterLink( DatabaseLink, 'mri_corrected', 'mri' )
    self.addParameterLink( DatabaseLink, 'field', 'mri_corrected' )
    self.addParameterLink( DatabaseLink, 'hfiltered', 'field' )
    self.addParameterLink( DatabaseLink, 'white_ridges', 'hfiltered' )
    self.addParameterLink( DatabaseLink, 'meancurvature', 'white_ridges' )
    self.addParameterLink( DatabaseLink, 'variance', 'meancurvature' )
    self.addParameterLink( DatabaseLink, 'edges', 'variance' )
