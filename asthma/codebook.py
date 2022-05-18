"""
This file contains the ICD 9/10 codes to extract asthma-related features
from the raw data from MSSQL server.

The history:
--05/19/2016: ICD 10-CM codes added
['J45.20','J45.21','J45.22',
 'J45.30','J45.31','J45.32',
 'J45.40','J45.41','J45.42',
 'J45.50','J45.51','J45.52',
 'J45.900','J45.901','J45.902','J45.909']

--03/2022: ICD 10-CM code added (Yolande confirmed on 6/2021)
['J82.33']
"""


# codes used for identifying asthma-related claims
ASTHMA_ICD_9_MIN_THRESH = '493'
ASTHMA_ICD_9_MAX_THRESH = '494'
ASTHMA_ICD_10_CM_CODES = [
    'J45.20','J45.21','J45.22',
    'J45.30','J45.31','J45.32',
    'J45.40','J45.41','J45.42',
    'J45.50','J45.51','J45.52',
    'J45.900','J45.901','J45.902',
    'J45.909', 'J82.33']


# codes used for identifying visit types
INPT_REV_CODES = (list(range(100, 220)) +
                  list(range(720, 725)) +
                  [729, 987])
ED_REV_CODES = [450, 451, 452, 456, 459, 981]
OUTPT_REV_CODES = (list(range(510, 524)) +
                   list(range(526, 530)) + 
                   list(range(570, 573)) +
                   list(range(579, 584)) +
                   [589, 590, 599, 982, 983])


INPT_POS_CODES = [6, 8, 21, 25, 26, 31, 32, 34, 51, 55, 61]
ED_POS_CODES = [23, 25, 26, 41, 42]
OUTPT_POS_CODES = [2, 3, 4, 5, 6, 7, 8, 9, 10,
                   11, 12, 13, 14, 15, 16, 17, 18, 19,
                   22, 24, 25, 26, 32, 33, 34, 49, 50,
                   52, 53, 54, 56, 57, 58, 60, 62, 65,
                   71, 72]
VIRTUAL_POS_CODES = [2, 10]


ALLERGIC_ICD_9_MIN_THRESH = '477.0'
ALLERGIC_ICD_9_MAX_THRESH = '477.9'
ALLERGIC_ICD_10_CODES = ['J30', 'J30.1', 'J30.2', 'J30.81', 'J30.89', 'J30.9']


OBESITY_ICD_9_MIN_THRESH = '278.0'
OBESITY_ICD_9_MAX_THRESH = '278.03'
OBESITY_ICD_10_CODES = ['E66.01', 'E66.09', 'E66.1', 'E66.2', 'E66.3', 'E66.8',
                        'E66.9']


OBS_SLEEP_ICD_9_CODE = '327.23'
OBS_SLEEP_ICD_10_CODE = 'G47.33'


GERD_ICD_9_CODE = '530.81'
GERD_ICD_10_CODES = ['K21.0', 'K21.9']


PLACE_OF_SERCVICE_NAMES = {
    1: 'Pharmacy',
    2: 'Telehealth (Provided Other Than Patient\'s Home)',
    3: 'School',
    4: 'Homeless Shelter',
    5: 'Indian Health Service Free-Standing Facility',
    6: 'Indian Helath Service Provider-Based Facility',
    7: 'Tribal 638 Free-Standing Facility (Not Require Hospitalization)',
    8: 'Tribal 638 Provider-Based Facility (Admitted as Inpatient or Outpatients)',
    9: 'Prison/Correctional Facility',
    10: 'Telehealth Provided in Patient\'s Home',
    11: 'Office',
    12: 'Home',
    13: 'Assisted Living Facility',
    14: 'Group Home',
    15: 'Mobile Unit',
    16: 'Temporary Lodging',
    17: 'Walk-in Retail Health Clinic',
    18: 'Place of Employment/Worksite',
    19: 'Off Campus-Outpatient Hospital',
    20: 'Urgent Care Facility',
    21: 'Inpatient Hospital',
    22: 'On Campus-Outpatient Hospital',
    23: 'Emergency Room-Hospital',
    24: 'Ambulatory Surgical Center',
    25: 'Birthing Center',
    26: 'Military Treatment Facility',
    27: 'Unassigned',
    28: 'Unassigned',
    29: 'Unassigned',
    30: 'Unassigned',
    31: 'Skilled Nursing Facility',
    32: 'Nursing Facility',
    33: 'Custodial Care Facility',
    34: 'Hospice',
    35: 'Unassigned',
    36: 'Unassigned',
    37: 'Unassigned',
    38: 'Unassigned',
    39: 'Unassigned',
    40: 'Unassigned',
    41: 'Ambulance - Land',
    42: 'Ambulance - Air or Water',
    43: 'Unassigned',
    44: 'Unassigned',
    45: 'Unassigned',
    46: 'Unassigned',
    47: 'Unassigned',
    48: 'Unassigned',
    49: 'Independent Clinic',
    50: 'Federally Qualified Health Center',
    51: 'Inpatient Psychiatric Facility',
    52: 'Psychiatric Facility - Partial Hospitalization',
    53: 'Community Mental Health Center',
    54: 'Intermediate Care Facility/Individuals with Intellectual Disabilities',
    55: 'Residential Substance Abuse Treatment Facility',
    56: 'Psychiatric Residential Treatment Center',
    57: 'Non-Residential Substance Abuse Treatment Facility',
    58: 'Non-Residential Opioid Treatment Facility',
    59: 'Unassigned',
    60: 'Mass Immunization Center',
    61: 'Comprehensive Inpatient Rehabilitation Facility',
    62: 'Comprehensive Outpatient Rehabilitation Facility',
    63: 'Unassigned',
    64: 'Unassigned',
    65: 'End-Stage Renal Disease Treatment Facility',
    66: 'Unassigned',
    67: 'Unassigned',
    68: 'Unassigned',
    69: 'Unassigned',
    70: 'Unassigned',
    71: 'State or Local Public Health Clinic',
    72: 'Rural Health Clinic',
    73: 'Unassigned',
    74: 'Unassigned',
    75: 'Unassigned',
    76: 'Unassigned',
    77: 'Unassigned',
    78: 'Unassigned',
    79: 'Unassigned',
    80: 'Unassigned',
    81: 'Independent Laboratory',
    82: 'Unassigned',
    83: 'Unassigned',
    84: 'Unassigned',
    85: 'Unassigned',
    86: 'Unassigned',
    87: 'Unassigned',
    88: 'Unassigned',
    89: 'Unassigned',
    90: 'Unassigned',
    91: 'Unassigned',
    92: 'Unassigned',
    93: 'Unassigned',
    94: 'Unassigned',
    95: 'Unassigned',
    96: 'Unassigned',
    97: 'Unassigned',
    98: 'Unassigned',
    99: 'Other Place of Service'
}

CONTROLLERS = [
    'AMINOPHYLLINE 100 MG ORAL TABLET',
    'AMINOPHYLLINE 200 MG ORAL TABLET',
    'BECLOMETHASONE 40 MCG/INH INHALATION AEROSOL',
    'BECLOMETHASONE 80 MCG/INH INHALATION AEROSOL',
    'BECLOMETHASONE DIPROPIONATE INHAL AERO SOLN 40 MCG/ACT',
    'BECLOMETHASONE DIPROPIONATE INHAL AERO SOLN 80 MCG/ACT',
    'BUDESONIDE 0.25 MG/2 ML INHALATION SUSPENSION',
    'BUDESONIDE 0.5 MG/2 ML INHALATION SUSPENSION',
    'BUDESONIDE 1 MG/2 ML INHALATION SUSPENSION',
    'BUDESONIDE 180 MCG/INH INHALATION POWDER',
    'BUDESONIDE 90 MCG/INH INHALATION POWDER',
    'BUDESONIDE INHAL AERO POWD 180 MCG/ACT (BREATH ACTIVATED)',
    'BUDESONIDE INHAL AERO POWD 90 MCG/ACT (BREATH ACTIVATED)',
    'BUDESONIDE INHALATION SUSP 0.25 MG/2ML',
    'BUDESONIDE INHALATION SUSP 0.5 MG/2ML',
    'BUDESONIDE INHALATION SUSP 1 MG/2ML',
    'BUDESONIDE-FORMOTEROL 160 MCG-4.5 MCG/INH INHALATION AEROSOL',
    'BUDESONIDE-FORMOTEROL 80 MCG-4.5 MCG/INH INHALATION AEROSOL',
    'BUDESONIDE-FORMOTEROL FUMARATE DIHYD AEROSOL 160-4.5 MCG/ACT',
    'BUDESONIDE-FORMOTEROL FUMARATE DIHYD AEROSOL 80-4.5 MCG/ACT',
    'CICLESONIDE CFC FREE 160 MCG/INH INHALATION AEROSOL',
    'CICLESONIDE CFC FREE 80 MCG/INH INHALATION AEROSOL',
    'CICLESONIDE INHAL AEROSOL 160 MCG/ACT',
    'CROMOLYN 0.8 MG/INH INHALATION AEROSOL WITH ADAPTER',
    'CROMOLYN 10 MG/ML INHALATION SOLUTION',
    'DYPHYLLINE 100 MG/15 ML ORAL ELIXIR',
    'DYPHYLLINE 200 MG ORAL TABLET',
    'DYPHYLLINE 400 MG ORAL TABLET',
    'DYPHYLLINE-GUAIFENESIN 100 MG-100 MG/15 ML ORAL LIQUID',
    'DYPHYLLINE-GUAIFENESIN 100 MG-100 MG/5 ML ORAL LIQUID',
    'DYPHYLLINE-GUAIFENESIN 100 MG-200 MG/5 ML ORAL SYRUP',
    'DYPHYLLINE-GUAIFENESIN 100 MG-50 MG/5 ML ORAL SYRUP',
    'DYPHYLLINE-GUAIFENESIN 200 MG-200 MG ORAL TABLET',
    'DYPHYLLINE-GUAIFENESIN 200 MG-300 MG ORAL TABLET',
    'DYPHYLLINE-GUAIFENESIN 200 MG-400 MG ORAL TABLET',
    'DYPHYLLINE-GUAIFENESIN 300 MG-300 MG/15 ML ORAL LIQUID',
    'FLUNISOLIDE 250 MCG/INH INHALATION AEROSOL',
    'FLUNISOLIDE 250 MCG/INH INHALATION AEROSOL WITH ADAPTER',
    'FLUTICASONE 100 MCG INHALATION POWDER',
    'FLUTICASONE 250 MCG INHALATION POWDER',
    'FLUTICASONE 50 MCG INHALATION POWDER',
    'FLUTICASONE CFC FREE 110 MCG/INH INHALATION AEROSOL',
    'FLUTICASONE CFC FREE 220 MCG/INH INHALATION AEROSOL',
    'FLUTICASONE CFC FREE 44 MCG/INH INHALATION AEROSOL',
    'FLUTICASONE PROPIONATE AER POW BA 100 MCG/BLISTER',
    'FLUTICASONE PROPIONATE AER POW BA 250 MCG/BLISTER',
    'FLUTICASONE PROPIONATE AER POW BA 50 MCG/BLISTER',
    'FLUTICASONE PROPIONATE HFA INHAL AER 110 MCG/ACT (125/VALVE)',
    'FLUTICASONE PROPIONATE HFA INHAL AER 220 MCG/ACT (250/VALVE)',
    'FLUTICASONE PROPIONATE HFA INHAL AERO 44 MCG/ACT (50/VALVE)',
    'FLUTICASONE-SALMETEROL 100 MCG-50 MCG INHALATION POWDER',
    'FLUTICASONE-SALMETEROL 250 MCG-50 MCG INHALATION POWDER',
    'FLUTICASONE-SALMETEROL 500 MCG-50 MCG INHALATION POWDER',
    'FLUTICASONE-SALMETEROL AER POWDER BA 100-50 MCG/DOSE',
    'FLUTICASONE-SALMETEROL AER POWDER BA 250-50 MCG/DOSE',
    'FLUTICASONE-SALMETEROL AER POWDER BA 500-50 MCG/DOSE',
    'FLUTICASONE-SALMETEROL CFC FREE 115 MCG-21 MCG/INH INHALATION AEROSOL',
    'FLUTICASONE-SALMETEROL CFC FREE 230 MCG-21 MCG/INH INHALATION AEROSOL',
    'FLUTICASONE-SALMETEROL CFC FREE 45 MCG-21 MCG/INH INHALATION AEROSOL',
    'FLUTICASONE-SALMETEROL INHAL AEROSOL 115-21 MCG/ACT',
    'FLUTICASONE-SALMETEROL INHAL AEROSOL 230-21 MCG/ACT',
    'FLUTICASONE-SALMETEROL INHAL AEROSOL 45-21 MCG/ACT',
    'FORMOTEROL-MOMETASONE 5 MCG-100 MCG/INH INHALATION AEROSOL',
    'FORMOTEROL-MOMETASONE 5 MCG-200 MCG/INH INHALATION AEROSOL',
    'GUAIFENESIN-THEOPHYLLINE 100 MG-100 MG/15 ML ORAL LIQUID',
    'GUAIFENESIN-THEOPHYLLINE 100 MG-300 MG/15 ML ORAL LIQUID',
    'MOMETASONE 110 MCG/INH INHALATION AEROSOL POWDER',
    'MOMETASONE 220 MCG/INH INHALATION AEROSOL POWDER',
    'MOMETASONE FUROATE INHAL POWD 110 MCG/INH (BREATH ACTIVATED)',
    'MOMETASONE FUROATE INHAL POWD 220 MCG/INH (BREATH ACTIVATED)',
    'MOMETASONE FUROATE-FORMOTEROL FUMARATE AEROSOL 100-5 MCG/ACT',
    'MOMETASONE FUROATE-FORMOTEROL FUMARATE AEROSOL 200-5 MCG/ACT',
    'MONTELUKAST 10 MG ORAL TABLET',
    'MONTELUKAST 4 MG ORAL GRANULE',
    'MONTELUKAST 4 MG ORAL TABLET, CHEWABLE',
    'MONTELUKAST 5 MG ORAL TABLET, CHEWABLE',
    'MONTELUKAST SODIUM CHEW TAB 4 MG (BASE EQUIV)',
    'MONTELUKAST SODIUM CHEW TAB 5 MG (BASE EQUIV)',
    'MONTELUKAST SODIUM ORAL GRANULES PACKET 4 MG (BASE EQUIV)',
    'MONTELUKAST SODIUM TAB 10 MG (BASE EQUIV)',
    'OMALIZUMAB 150 MG SUBCUTANEOUS INJECTION',
    'THEOPHYLLINE 100 MG ORAL TABLET, EXTENDED RELEASE',
    'THEOPHYLLINE 100 MG/24 HOURS ORAL CAPSULE, EXTENDED RELEASE',
    'THEOPHYLLINE 125 MG ORAL CAPSULE, EXTENDED RELEASE',
    'THEOPHYLLINE 200 MG ORAL CAPSULE, EXTENDED RELEASE',
    'THEOPHYLLINE 200 MG ORAL TABLET, EXTENDED RELEASE',
    'THEOPHYLLINE 200 MG/24 HOURS ORAL CAPSULE, EXTENDED RELEASE',
    'THEOPHYLLINE 300 MG ORAL CAPSULE, EXTENDED RELEASE',
    'THEOPHYLLINE 300 MG ORAL TABLET, EXTENDED RELEASE',
    'THEOPHYLLINE 300 MG/24 HOURS ORAL CAPSULE, EXTENDED RELEASE',
    'THEOPHYLLINE 400 MG/24 HOURS ORAL CAPSULE, EXTENDED RELEASE',
    'THEOPHYLLINE 400 MG/24 HOURS ORAL TABLET, EXTENDED RELEASE',
    'THEOPHYLLINE 450 MG ORAL TABLET, EXTENDED RELEASE',
    'THEOPHYLLINE 600 MG/24 HOURS ORAL TABLET, EXTENDED RELEASE',
    'THEOPHYLLINE 80 MG/15 ML ORAL ELIXIR',
    'THEOPHYLLINE 80 MG/15 ML ORAL SOLUTION',
    'TRIAMCINOLONE 75 MCG/INH INHALATION AEROSOL',
    'ZAFIRLUKAST 10 MG ORAL TABLET',
    'ZAFIRLUKAST 20 MG ORAL TABLET',
    'ZILEUTON 600 MG ORAL TABLET',
    'ZILEUTON 600 MG ORAL TABLET, EXTENDED RELEASE']
    
