BEGIN {
        print "INSERT INTO `rms_forecast_detail`(`htl_cd`,`para_typ`,`para_cd`,`order_dt`,`live_dt`,`fc_dt`,`fc_occ`,`fc_rev`,`update_dt`) VALUES "
}
{
        if (NR != 1) {
                print ","
        }
        print "('" $1 "','" $2 "','" $3 "','" $4 "','" $5 "','" $6 "','" $7 "','" $8 "',now())"
}
