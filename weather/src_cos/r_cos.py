#!/usr/bin/env python

import sys
import numpy as np

# napa lines:
napa_1990 = "0.2269450289079499,0.1552107014026266,0.43850925018109727,0.21654131192948897,0.12285884994876128,-0.21736825180217303,0.2466324423401085,-0.7416450210624369,0.4423227411818101,-0.039188798489465757,0.6998313499744201,-0.2019348720922561,0.09008091217627236,-0.6825252500131158,0.32960190128605105,0.14572296202068769,0.09493197649371854,-0.2415868992629714,-0.013323701923873159,0.11257319707633881,0.10078726536723542,0.5656844422868312,0.0691349929996873,-0.2503437232011359,-0.3373904916431636,-0.24024100345522592,0.0019808699889471704,1.0349992424847765,-0.0500392011700295,-0.2608660846418956,-0.3864404201942056,0.06208957548435161,-0.0969606309095026,1.3479583555019234,-0.18110515151892081,-0.24994005407469794,-0.46108514933124184,0.5321809672208868,-0.24285794558227516,1.4995684437463042,-0.20320459775764724,-0.2984513348867722,-0.24240732992936762,0.7595335737439108,-0.04978989547718379,1.1813909352817078,-0.16133165786891557,-0.29351261390903305,-0.01060356322794164,0.3773489992497017,0.11981995373918458,0.5956325755783003,-0.19406083734184917,-0.2726776698165141,0.18700615773211232,0.6796986267410293,0.040597073448935546,0.021127771839455325,-0.07695733831978083,-0.24817158137915651,0.22413406998982782,-0.6320085730609855,0.45533490997878756,-0.22623527691136128,0.03770199624323794,-0.19113981713423142,0.15047418052540013,0.01268850788114826,0.20920231660909966,-0.04051089441948861,-0.007454138969241638,-0.22636397935215613"
napa_2000 = "0.2904108150181677,-0.6434337345723474,0.4400895550426081,-0.22319403247572292,1.5442818201927295,-0.13340520525689303,0.3267303260155467,-0.6607643980757805,0.4769487051540391,-0.26514584608897723,2.311095923613402,-0.08538176658941346,0.24671927951499423,-0.35317312766352554,0.40973771386855656,-0.18207787142491233,0.9523521152719421,-0.18670732426764636,0.007295816185170147,-0.273230997383551,0.20604610845791077,0.14914280368672617,0.3228635556464415,-0.23300326247579706,-0.06715161339541474,0.401872394544295,0.11434698112165999,0.4510569171787573,0.17591014572287497,-0.2514300674097733,-0.17137823132299054,0.7140107875098927,-0.16056310314290703,0.8466322186544145,-0.14447686523559508,-0.24707399626057383,-0.35078440480390444,0.6563452448623377,-0.19949117761547552,0.9519645664915071,-0.2041354696341099,-0.2983211703204863,-0.293403279122759,0.6787661533562712,-0.18379277604106165,0.7187336730212405,-0.15843104661744623,-0.2921299421324863,0.0291289093087375,1.0233557337357395,-0.05717701378094939,0.1228553482978153,-0.17438670623613442,-0.27067786374239033,0.15140623136062284,0.23380898132364528,0.08550183420041092,-0.2541445942735833,0.29779020121989275,-0.22077763214395513,0.196709134913936,0.03170736462996603,0.2857908365268468,-0.49104795116449373,0.07524456009797308,-0.17766316221137063,0.2360802975329223,-0.47640374287867404,0.38598557971638264,-0.19470143331722145,0.19815631616670143,-0.08301717082261253"
napa_2010 = "0.33980304225502167,-0.4719609270221972,0.46266829129896236,-0.36408837311335446,2.1016849613993585,-0.09421712785144279,0.35556935634299586,-0.32584287535124934,0.44926827879993236,-0.3320878129877358,2.0074206631363247,-0.10642475917983248,0.27374350049549134,-0.40244945544242483,0.44672338889706265,-0.2935574448114679,2.130785626809432,-0.10481246978704309,0.10116987032000031,-0.2788735815010987,0.30580951209734303,-0.006268951449825443,0.7930486327288604,-0.19370058760587494,-0.12184747503974386,0.15990817483068695,0.07558542665927923,0.515237150790441,-0.016671433432133162,-0.26244164236020867,-0.16071292332247306,0.5498816838090537,-0.07824047232127998,0.767401868472089,-0.09224316325538615,-0.24135239431021666,-0.36901419917542744,0.6563452448623377,-0.18329373529085877,0.9005303629490866,-0.2068193028197505,-0.29424529271300115,-0.33474192550935455,0.43646389219335247,-0.11174531728458921,0.7394437188110713,-0.16483217558797994,-0.29337820783805774,0.0728725423439007,0.8233680477537407,0.061272350238899584,0.09336073840875717,-0.1267507186672474,-0.26759441686968344,0.2586989559814004,0.4372341536045629,0.20772122895270848,-0.40816672476215027,0.30813960498953086,-0.22112685330907864,0.24223605042066423,0.0876944410325975,0.2768138333721357,-0.5192878219605668,0.09253884300017484,-0.176825229838602,0.25866063358400526,-0.062316958815236474,0.31142851004995825,-0.3836850592117758,0.1430801021237248,-0.11901096498239029"

# helpers
def str_to_np(in_str):
    a = in_str.strip().split(",")
    py_list = []
    for i in a:
        py_list.append(float(i))
    return np.array(py_list)


def cos_sim(v1, v2):
    return v1.dot(v2) / np.linalg.norm(v1) / np.linalg.norm(v2)


v_napa_1990 = str_to_np(napa_1990)
v_napa_2000 = str_to_np(napa_2000)
v_napa_2010 = str_to_np(napa_2010)

for l in sys.stdin:
    k, v = l.strip().split("\t")

    if 'None' not in v and '\\N' not in v:
        np_v = str_to_np(v)

        d1990 = cos_sim(v_napa_1990, np_v)
        d2000 = cos_sim(v_napa_2000, np_v)
        d2010 = cos_sim(v_napa_2010, np_v)

        print("%s,1990,%s" % (k, d1990))
        print("%s,2000,%s" % (k, d2000))
        print("%s,2010,%s" % (k, d2010))





