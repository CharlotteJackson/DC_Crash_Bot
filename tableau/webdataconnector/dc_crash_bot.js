(function () {
    var myConnector = tableau.makeConnector();

    myConnector.getSchema = function (schemaCallback) {

        var cols = [{
            id: "objectid",
            dataType: tableau.dataTypeEnum.int
        }, {
            id: "fromdate",
            alias: "fromdate",
            dataType: tableau.dataTypeEnum.date
        }, {
            id: "latitude",
            alias: "latitude",
            dataType: tableau.dataTypeEnum.float
        },

        {
            id: "longitude",
            alias: "longitude",
            dataType: tableau.dataTypeEnum.float
        },
        {
            id: "address",
            alias: "address",
            dataType: tableau.dataTypeEnum.string
        },
        {
            id: "ward_name",
            alias: "ward_name",
            dataType: tableau.dataTypeEnum.string
        },
        {
            id: "nbh_cluster_names",
            alias: "nbh_cluster_names",
            dataType: tableau.dataTypeEnum.string
        },
        {
            id: "comp_plan_area",
            alias: "comp_plan_area",
            dataType: tableau.dataTypeEnum.string
        },
        {
            id: "crash_type",
            alias: "crash_type",
            dataType: tableau.dataTypeEnum.string
        },
        {
            id: "crash_on_interstate",
            alias: "crash_on_interstate",
            dataType: tableau.dataTypeEnum.int
        },
        {
            id: "scanner_audio_missing",
            alias: "scanner_audio_missing",
            dataType: tableau.dataTypeEnum.bool
        },
        {
            id: "crash_category",
            alias: "crash_category",
            dataType: tableau.dataTypeEnum.string
        },
        {
            id: "national_park",
            alias: "national_park",
            dataType: tableau.dataTypeEnum.int
        },
        {
            id: "geography",
            dataType: tableau.dataTypeEnum.geometry
        }];

        var tableSchema = {
            id: "dc_crash_bot_crashes",
            alias: "Crash data",
            columns: cols
        };

        schemaCallback([tableSchema]);


    };

    myConnector.getData = function (table, doneCallback) {
        $.getJSON("https://raw.githubusercontent.com/CharlotteJackson/DC_Crash_Bot/tableau_web_connector/tableau/webdataconnector/data/february_2021_crashes.json", function (resp) {
            var feat = resp.crashes
            var tableData = [];

            // Iterate over the JSON object
            for (var i = 0, len = feat.length; i < len; i++) {
                tableData.push({
                    "objectid": feat[i].objectid,
                    "fromdate": feat[i].fromdate,
                    "geography": feat[i].geography,
                    "latitude": feat[i].latitude,
                    "longitude": feat[i].longitude,
                    "address": feat[i].address,
                    "ward_name": feat[i].ward_name,
                    "nbh_cluster_names": feat[i].nbh_cluster_names,
                    "comp_plan_area": feat[i].comp_plan_area,
                    "crash_type": feat[i].crash_type,
                    "crash_on_interstate": feat[i].crash_on_interstate,
                    "scanner_audio_missing": feat[i].scanner_audio_missing,
                    "crash_category": feat[i].crash_category,
                    "national_park": feat[i].national_park,

                });
            }

            table.appendRows(tableData);
            doneCallback();
        });
    };


    tableau.registerConnector(myConnector);


    $(document).ready(function () {
        $("#submitButton").click(function () {
            tableau.connectionName = "DC Crashbot Feed";
            tableau.submit();
        });
    });


})();
