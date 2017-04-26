#framework name
name = "rms-create-version-v3"

#mail configurations
mail{
  api.url="http://42.62.78.140:8002/WarningEmail/"
  #multi receiver, such as ["a1@b.com","a2@b.com"]
  #to=["jbnie@jointwisdom.cn"]
  to=["xnzhang@jointwisdom.cn","yclai@jointwisdom.cn"]
}


#assembly的存放目录的绝对路径
assemblies-dir = "/data/work/bidev/create_version_v3/assemblies/"
#所有assembly的配置信息
assemblies = [
  {
    name = "gen-config"
    index = 1
    #options ["cleaner","model"]
    type = "cleaner"
    jar-name = "rms-ab-micro-pred-gen-config-assembly-1.0.jar"
    class-name = "cn.jw.rms.pred.genconfig.Cleaner"
    enable = true
  },
  {
    name = "create-version"
    index = 2
    #options ["cleaner","model"]
    type = "model"
    jar-name = "rms-ab-create-version-v2-assembly-1.0.jar"
    class-name = "cn.jw.rms.version.creation.Predictor"
    enable = true
  }
]

#assembly的参数配置
parameters = [
  {
    #(必填)assembly的唯一名称, 标识该参数列表属于哪个assembly
    name = "gen-config"
    #以下就为具体的参数
    version = "daily_20160809"
    config-dist-dir = "/data/work/bidev/version_templates"
    hotel-list = ["221065", "DJSW000001","htl3","htl4"]
    filed-splitter = "#"
    simple-config-list = [
      {
        name = "htlcd"
        src-dir = "hdfs://ns1/p/bw/bi/rms/rms_ref_hotel/dt=history"
      },
      {
        name = "invt"
        src-dir = "hdfs://ns1/p/bw/bi/rms/rms_inventory"
        dt = "now"
      },
      {
        name = "specialday"
        src-dir = "/data/work/bidev/create_version_v3/tpldata/specialday"
      },
      {
        name = "season"
        src-dir = "/data/work/bidev/create_version_v3/tpldata/season"
      },
      {
        name = "invt_field_config"
        src-dir = "/data/work/bidev/create_version_v3/tpldata/invt_field_config"
      }
    ]

    complex-config-list = [
      {
        name = "predconf"
        src-list = [
          {
            name = "segcd"
            dir = "hdfs://ns1/p/bw/bi/rms/rms_ref_market_seg/dt=20160110"
            field-list = ["id", "htl_cd", "seg_cd"]
          }
        ]
      },
      {
        name = "is_predict"
        src-list = [
          {
            name = "segcd"
            dir = "hdfs://ns1/p/bw/bi/rms/rms_ref_market_seg/dt=20160110"
            field-list = ["id", "htl_cd", "seg_cd"]
          }
        ]
      }
    ]
    
  },
  {
    #(必填)assembly的唯一名称, 标识该参数列表属于哪个assembly
    name = "create-version"
    #以下就为具体的参数
    hadoop-host= "hdfs://ns1"
    field-splitter = "#"
    root-version = "daily_20160809"
    template-root-dir = "/data/work/bidev/version_templates"
    #conf-root-dir = "hdfs://ns1/p/bw/bi/rms/conf/"
    conf-root-dir = "hdfs://ns1/p/bw/bi/rms/conf/"
    dist-root-dir = "/data/work/bidev/version_dist/"
    hist-data-dir = "hdfs://ns1/p/bw/bi/rms/seg_bk_daily_sum/dt=20160808"
    upload-conf-enable = true
    train-ratio = 0.7
    wsfweight-days = 365
    parallelism = 2
    static-kv-list = [
      {
        name = "V_SEG_BK_DAILY_SUM_HIST"
        value = "20160808"
      },
      {
        name = "V_ADV_FC_DAYS"
        value = "31"
      },
      {
        name = "V_ENABLE_COPY"
        value = "true"
      }
    ]
  }
]
