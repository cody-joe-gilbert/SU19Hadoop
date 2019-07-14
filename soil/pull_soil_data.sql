# survey areas 
SELECT l.areasymbol, l.lkey, l.areaname, l.mlraoffice, l.areaacres, 
	a.mbrminx, a.mbrminy, a.mbrmaxx, a.mbrmaxy
FROM legend l
JOIN sacatalog a ON a.areasymbol = l.areasymbol
WHERE l.areatypename = 'Non-MLRA Soil Survey Area' 
ORDER BY 1 

# mapunits 
SELECT mukey, muname, mukind, muacres, farmlndcl, museq, nationalmusym, lkey
FROM mapunit 

# components 
SELECT cokey, compname, mukey, comppct_r, localphase, slope_r, compkind, majcompflag, drainagecl, taxpartsize, 
	runoff, tfact, wei, erocl, hydricrating, elev_r, aspectrep, nirrcapcl, irrcapcl, frostact, hydgrp, 
	taxceactcl, taxreaction, taxtempcl
FROM component 

# horizons/layers 
SELECT chkey, cokey, hzdept_r, hzdepb_r, hzname, hzthk_r,
	fraggt10_r, frag3to10_r, sieveno4_r, sieveno10_r, sieveno40_r, sieveno200_r,
	sandtotal_r, sandvc_r, sandco_r, sandmed_r, sandfine_r, sandvf_r, 
	silttotal_r, siltco_r, siltfine_r, claytotal_r, claysizedcarb_r, om_r, 
	awc_r, wtenthbar_r, wthirdbar_r, wfifteenbar_r, wsatiated_r, 
	kwfact, kffact, caco3_r, gypsum_r, sar_r, ec_r, cec7_r, ecec_r, sumbases_r, 
	ph1to1h2o_r, ph01mcacl2_r, freeiron_r, feoxalate_r, ptotal_r
FROM chorizon 
