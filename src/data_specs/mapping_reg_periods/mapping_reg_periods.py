"""Map the individual ratings into broader groups (AAA, AA, A, BBB, BB, B, C, NR). This is important to
harmonize ratings across different rating agencies.

In a next step I map the different rating categories to the respective risk-weights
"""

# The mapping is set up based on the actual ratings that appear in the data.
remap_ratings_dict={
    # triple A
    'AAA':'AAA', 'Aaa':'AAA',
    # double A
    'Aa1':'AA',  'Aa2':'AA',  'Aa3':'AA',  'AA+':'AA',  'AA-':'AA',  'AA':'AA',
    # A
    'A':'A', 'A+':'A', 'A-':'A','A1':'A', 'A2':'A', 'A3':'A',
    # tripe B
    'BBB+':'BBB', 'BBB':'BBB', 'BBB-':'BBB','Baa1':'BBB', 'Baa2':'BBB', 'Baa3':'BBB', 'Baa':'BBB',
    # double B
    'BB+':'BB', 'BB':'BB', 'BB-':'BB','Ba1':'BB', 'Ba2':'BB', 'Ba3':'BB',
    #  B
    'B+':'B', 'B':'B', 'B-':'B','B1':'B', 'B2':'B', 'B3':'B',
    #  triple C
    'CCC+':'CCC', 'CCC':'CCC', 'CCC-':'CCC','Caa1':'CCC', 'Caa2':'CCC', 'Caa3':'CCC',
    #  double C
    'CC':'CC', 'Ca':'CC',
    #  C
    'C':'C',
    #  DDD
    'DDD':'DDD',
    #  DD
    'DD':'DD',
    #  D
    'D':'D',
    #  NR
    'NR':'NR'
}

# Map the bond ratings into risk weights. Note: Unrated bonds are in risk category 4
remap_ECRA_risk_weights={
    # risk weight = 20 %
    'AAA': 1, 'AA':1, 'A':2, 'BBB':3, 'BB':4, 'B':4, 'CCC':5, 'CC':5, 'C':5, 'D':5, 'DD':5, 'DDD':5, 'NR':4
}


# Map the categorical risk weights into descriptive risk weights
remap_ECRA_risk_weights_names={
    1: '20% (AAA - AA-)', 2:'50%  (A+ - A-)', 3:'75% (BBB+ - BBB-)', 4:'100% (BB+ - B-)', 5:'150% (Below B-)'
}

# Assign integer numbers to each individual rating. This is important for calculating average ratings
remap_ratings_integer_dict={
    # triple A
    'AAA':1, 'Aaa':1,
    # double A
    'Aa1':2,  'Aa2':3,  'Aa3':4,  'AA+':2,  'AA-':3,  'AA':4,
    # A
    'A':6, 'A+':5, 'A-':7,'A1':5, 'A2':6, 'A3':7,
    # tripe B
    'BBB+':8, 'BBB':9, 'BBB-':10,'Baa1':8, 'Baa2':9, 'Baa3':10, 'Baa':9,
    # double B
    'BB+':11, 'BB':12, 'BB-':13,'Ba1':11, 'Ba2':12, 'Ba3':13,
    #  B
    'B+':14, 'B':15, 'B-':16,'B1':14, 'B2':15, 'B3':16,
    #  triple C
    'CCC+':17, 'CCC':18, 'CCC-':19,'Caa1':17, 'Caa2':18, 'Caa3':19,
    #  remaining C
    'CC':20, 'Ca':20, 'C':21,
    # D
    'DDD':22, 'DD':23, 'D':24, 'NR':25
}


