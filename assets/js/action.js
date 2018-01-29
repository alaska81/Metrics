var left_menu = document.getElementById("left_menu"); //left_menu
var Config = {
	Metrics_mod: {
		Metrics_mod: [],
		Metrics_mod_option:'',
	},
	Metrics_type: {
		Metrics_type: [],
		Metrics_type_option:'',
	},
	Metrics_hierarchy: {
		Metrics_hierarchy: [],
		Metrics_hierarchy_option:'',
	},
	Users:{
		Users: [],
		UsersOption: ' ',
	}
};

var Parameter = {
	Rashod: 9,
	Prixod: 10,
	Spisanie: 11,
	AverageOrderAmountPerDay:12,
	TheNumberOfOrders: 13,
	TheNumberOfRefusalsWithWriteOffs: 14,
	TheNumberOfFailuresWithoutCharge: 15,
	TheNumberOfSumPricesExecutedOrders: 47,
	ReportSale: 42,
	Courier: 43,
	Operator: 40,
	Cashbox: 44,
	TheNumberOfSumPricesExecutedOrders: 47,
	ReportSaleNew: 48,
}

/*
9  1  ""
10 8  ""
11 13 ""
12 16 "1. Средняя сумма заказа на день. (ценник)"
13 18 "3.1 Запрос на количество заказов за день на точке."
14 20 "3.2 Запрос на количество отказов со списанием за день на точке."
15 21 "3.3 Запрос на количество отказов со БЕЗ списанием за день на точке."
18 22 "4. Заказов по типу: доставка/доставка ко времени/навынос/в ресторане за день."
23 28 "8. Количество доставленных заказов ВОВРЕМЯ за день"
24 29 "9. Количество заказов к которым применеты скидки и акции"
25 30 "9. Количество заказов к которым НЕ БЫЛИ применены скидки и акции "
26 31 "10. На какую сумму было использовано скидок за день *"
27 32 "14. Количество блюд на переделке по точкам за день"
28 33 "15. Количество заказов по блюду на точке за день "
29 34 "22. Количество ожидающих доставку каждый определенный интервал минут на точке.
 вметрике смотрим весь список точек и бегаем по ним "
30 35 "ПОВАР: Запрос на кто какого блюда сколько приготовил"
31 36 "ПОВАР: На переделку кто ставил"
32 37 "ПОВАР: Переделка"
33 40 "5.1 КОЛИЧЕСТВО По способу оплаты: наличные/по карте/электронные деньги за день"
34 41 "5.1 СУММА По способу оплаты: наличные/по карте/электронные деньги за день"
35 42 "6. Количество заказов по каналу оформления заказа: в ресторане/колл-центр/приложение/сайт"
36 45 "11. Среднее время приготовления заказов за день"
37 46 "13. Среднее время доставки заказа за день "
38 47 "17. Среднее время ожидания выполнения заказов за час на точке. ( У Николая его нет )"
39 48 "20. Количество заказаов в очереди за час на точке. "
40 49 "1. Количество оформленных заказов за день"
41 54 "Кто сколько отработал"
42 58 "Какие эл. менб по точкам сколько и цена"
43 61 "Массивы ID доставленных заказов по курьерам + количество"
44 63 "Кассовый отчёт"
*/


var menu_administration = [
    {name:"Типы и модификаторы",tab:"#evPanel1"},
    {name:"Связь типов и модификаторов",tab:"#evPanel2"},
    {name:"Параметры метрики",tab:"#evPanel2"}];
var Link_action_obj;


$('#point_count_refusals_btn').on('click', function(){
    let TableArray = [];


let filter_date_start = $('#filter_date_start').val();
let filter_date_end = $('#filter_date_end').val();
	
	TableArray.push( JSON.stringify(new Struct_Table("Select", "CancellationOrder", "CanceledOrders", -1, 99999,0, [filter_date_start])));
	Link_action_obj.Select('CancellationOrder',TableArray);
	$('#myModalCancel').modal('show');
	
});

$(document).ready(function(){
	//$('#filter_date, #filter_date_start, #filter_date_end').val(GetDates("gggg-mm-dd"));
	$('#filter_date_start').val(GetDates("gggg-mm-dd"));	

	Link_action.prototype.Config(['Users']);

    Link_action_obj = new Link_action();

    let TableArray = [];

    TableArray.push( JSON.stringify(new Struct_Table("Select", "metrics_type", "", -1, 99999, 0)));
    TableArray.push( JSON.stringify(new Struct_Table("Select", "metrics_mod", "", -1, 99999, 0)));
    TableArray.push( JSON.stringify(new Struct_Table("Select", "metrics_link_type_and_mod_or_names", "", -1, 99999, 0)));
    TableArray.push( JSON.stringify(new Struct_Table("Select", "metrics_dop_data", "LoadHierarchy", -1, 99999, 0, ['0'])));
   	Link_action_obj.Select('ready',TableArray);

//	TableArray = [];
// 	TableArray.push( JSON.stringify(new Struct_Table("Select", "CancellationOrder", "CanceledOrders", -1, 99999,0, ["2017-12-04"])));
//  Link_action_obj.Select('ready',TableArray);

    left_menu.innerHTML = "";
    for(let i in menu_administration)
    {
        left_menu.innerHTML += "<li role=\"presentation\"><a class=\"tabnav\" data-toggle=\"tab\" href="+menu_administration[i].tab+">"+menu_administration[i].name+"</a></li>";
    }

	$('#TabSelectChain').change(function(){
		let ul = $(this).closest('ul');
		if ($(this).find('option:selected').attr('data-id') != '-') {
				ul.find('#TabOrg').removeClass('hide').end()
					.find('#TabPoint').addClass('hide').end()
					.find('#TabSelectOrg').find('option').remove();
			let TableArray = [];
			TableArray.push( JSON.stringify(new Struct_Table("Select", "metrics_dop_data", "LoadHierarchy", -1, 99999, 0, [$(this).find('option:selected').attr('data-hash')])));
   			Link_action_obj.Select('org',TableArray);
		} else {
			ul.find('#TabOrg').addClass('hide').end()
				.find('#TabPoint').addClass('hide');
		}
	});

	$('#TabSelectOrg').change(function(){
		let ul = $(this).closest('ul');
		if ($(this).find('option:selected').attr('data-id') != '-') {
			ul.find('#TabPoint').removeClass('hide').end()
				.find('#TabSelectPoint').find('option').remove();
			let TableArray = [];
				TableArray.push( JSON.stringify(new Struct_Table("Select", "metrics_dop_data", "LoadHierarchy", -1, 99999, 0, [$(this).find('option:selected').attr('data-hash')])));
	   			Link_action_obj.Select('point',TableArray);
		} else {
			ul.find('#TabPoint').addClass('hide');
		}
	});

	$('#TabSelectPoint').change(function(){
		let ul = $(this).closest('ul');
		if ($(this).find('option:selected').attr('data-id') != '-') {
			//let TableArray = [];
			//TableArray.push( JSON.stringify(new Struct_Table("Select", "metrics_dop_data", "LoadHierarchy", 99999, 0, [$(this).find('option:selected').attr('data-hash')])));
			//Link_action_obj.Select('point',TableArray);
			let TArray = [],
				Hash = $('#TabSelectPoint').find('option:selected').attr('data-hash'),
				//StartDate = GetFirstDates("gggg-mm-dd"),
				//EndDate = GetDates("gggg-mm-dd");
				StartDate = $('#filter_date_start').val(),
				EndDate = $('#filter_date_end').val();
				if(EndDate == "") EndDate = StartDate;
				TArray.push( JSON.stringify(new Struct_Table("Select", "metrics_dop_data", "LoadHierarchy", -1, 99999, 0, [Hash])));
				TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ParametersByInterval", Parameter.AverageOrderAmountPerDay, 99999, 0, [Parameter.AverageOrderAmountPerDay, Hash, StartDate, EndDate])));
				TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ParametersByInterval", Parameter.TheNumberOfOrders, 99999, 0, [Parameter.TheNumberOfOrders, Hash, StartDate, EndDate])));
				TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ParametersByInterval", Parameter.TheNumberOfRefusalsWithWriteOffs, 99999, 0, [Parameter.TheNumberOfRefusalsWithWriteOffs, Hash, StartDate, EndDate])));
				TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ParametersByInterval", Parameter.TheNumberOfFailuresWithoutCharge, 99999, 0, [Parameter.TheNumberOfFailuresWithoutCharge, Hash, StartDate, EndDate])));

				Link_action_obj.Select('sklad', TArray);

				//$('#btn_action_filter_interval').click(); // автоклик при выборе точке
		} else { //Сюда если не выбрана точка
			//ul.find('#TabPoint').addClass('hide');
		}
	});

	//Для скрытия ненужных данных
	$('#select_action_point').change(function(){
		//console.log('select_action_point:', $(this).find('option:selected').attr('data-id'))
		switch ( $(this).find('option:selected').attr('data-id') ) {
			case 'ReportSale': case 'Cashbox': {
				$('#ReportSale').removeClass('hide')
				break;
			}
			case 'Rashod': case 'Prixod': case 'Spisanie': case 'Courier': case 'Operator':{
				$('#ReportSale').addClass('hide')
				break;
			}
			default:
				break;
		}
	});


//	$('#ReportSaleElements').on('click',function(){
//		$('#table_report thead').append('<th>ID</th><th>Имя</th><th>Количество</th><th>Цена</th>');
//		let TArray = [];
//			TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ReportSale", 99999, 0, [$('#TabSelectPoint').find('option:selected').attr('data-hash')])));
//	   		Link_action_obj.Select('ReportSale',TArray);

//	});


//	$('#btn_action_filter').on('click', function(){
//		$('#table_report').removeClass('display-none');
//		$('#table_report').find('thead tr th, tbody tr td').remove();
//		let Point = $('#TabSelectPoint').find('option:selected').attr('data-hash'),
//			Sklad = $('#TabPoint').attr('data-sklad'),
//			DateS = $('#filter_date').val();
//		$('#ReportSert').addClass('hidden');
//		$('#Report_Text_Sale').text('Общая сумма продаж:');
//		switch ($('#select_action_point').find('option:selected').attr('data-id')) {
//			case 'ReportSale': {
//				let TArray = [],
//					Hash = Point,
//					StartDate = DateS,
//					EndDate = DateS;
//					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics_dop_data", "LoadHierarchy", -1, 99999, 0, [Hash])));
//					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ParametersByInterval", Parameter.AverageOrderAmountPerDay, 99999, 0, [Parameter.AverageOrderAmountPerDay, Hash, StartDate, EndDate])));
//					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ParametersByInterval", Parameter.TheNumberOfOrders, 99999, 0, [Parameter.TheNumberOfOrders, Hash, StartDate, EndDate])));
//					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ParametersByInterval", Parameter.TheNumberOfRefusalsWithWriteOffs, 99999, 0, [Parameter.TheNumberOfRefusalsWithWriteOffs, Hash, StartDate, EndDate])));
//					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ParametersByInterval", Parameter.TheNumberOfFailuresWithoutCharge, 99999, 0, [Parameter.TheNumberOfFailuresWithoutCharge, Hash, StartDate, EndDate])));
//					Link_action_obj.Select('sklad', TArray);

//				TArray = [];
//					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ReportSale", -1, 99999, 0, [Parameter.ReportSale, Point, DateS])));
//					Link_action_obj.Select('ReportSale',TArray);
//				break;
//			}
//			case 'Rashod': {
//				let TArray = [];
//					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "Report", Parameter.Rashod, 99999, 0, [Parameter.Rashod, Sklad, DateS])));
//					Link_action_obj.Select('ReportSale',TArray);
//				break;
//			}
//			case 'Prixod': {
//				let TArray = [];
//					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "Report", Parameter.Prixod, 99999, 0, [Parameter.Prixod, Sklad, DateS])));
//					Link_action_obj.Select('ReportSale',TArray);
//				break;
//			}
//			case 'Spisanie': {
//				let TArray = [];
//					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "Report", Parameter.Spisanie, 99999, 0, [Parameter.Spisanie, Sklad, DateS])));
//					Link_action_obj.Select('ReportSale',TArray);
//				break;
//			}
//			case 'Courier': {
//				let TArray = [];
//					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ReportCourier", Parameter.Courier, 99999, 0, [Parameter.Courier, Point, DateS])));
//					Link_action_obj.Select('ReportCourier',TArray);
//				break;
//			}
//			case 'Operator': {
//				let TArray = [];
//					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ReportOperator", Parameter.Operator, 99999, 0, [Parameter.Operator, DateS])));
//					Link_action_obj.Select('ReportOperator',TArray);
//				break;
//			}
//			case 'Cashbox': {
//				$('#ReportSert').removeClass('hidden');
//				$('#Report_Text_Sale').text('Касса:');
//				let TArray = [];
//					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ReportCashbox", Parameter.Cashbox, 99999, 0, [Parameter.Cashbox, Point, DateS])));
//					Link_action_obj.Select('ReportCashbox',TArray);
//				break;
//			}
//			default:
//				break;
//		}
//	});

	$('#table_report').addClass('display-none');

	$('#btn_action_filter_interval').on('click', function(){
		$('#table_report').removeClass('display-none');
		$('#table_report').find('thead tr th, tbody tr td').remove();
		let Point = $('#TabSelectPoint').find('option:selected').attr('data-hash'),
			Sklad = $('#TabPoint').attr('data-sklad'),
			DateStart = $('#filter_date_start').val(),
			DateEnd = $('#filter_date_end').val();
			if(DateEnd == "") DateEnd = DateStart; //оптимизация, избавляющая от отдельной кнопки с датой
		$('#ReportSert').addClass('hidden');
		$('#Report_Text_Sale').text('Общая сумма продаж: ');
		switch ($('#select_action_point').find('option:selected').attr('data-id')) {
			case 'ReportSale': { //Отчет по продажам
				let TArray = [],
					Hash = Point;
					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics_dop_data", "LoadHierarchy", -1, 99999, 0, [Hash])));
					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ParametersByInterval", Parameter.AverageOrderAmountPerDay, 99999, 0, [Parameter.AverageOrderAmountPerDay, Hash, DateStart, DateEnd])));
					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ParametersByInterval", Parameter.TheNumberOfOrders, 99999, 0, [Parameter.TheNumberOfOrders, Hash, DateStart, DateEnd])));
					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ParametersByInterval", Parameter.TheNumberOfRefusalsWithWriteOffs, 99999, 0, [Parameter.TheNumberOfRefusalsWithWriteOffs, Hash, DateStart, DateEnd])));
					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ParametersByInterval", Parameter.TheNumberOfFailuresWithoutCharge, 99999, 0, [Parameter.TheNumberOfFailuresWithoutCharge, Hash, DateStart, DateEnd])));
					Link_action_obj.Select('sklad', TArray);

				TArray = [];
					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ReportCashboxByInterval", Parameter.Cashbox, 99999, 0, [Parameter.Cashbox, Hash, DateStart, DateEnd])));
					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ReportSaleByInterval", -1, 99999, 0, [Parameter.ReportSale, Hash, DateStart, DateEnd])));
					Link_action_obj.Select('ReportSale',TArray);
				break;
			}
			case 'ReportSaleNew': {
				let TArray = [];

				let DateStartNew = new Date(DateStart);
				//DateStartNew.setHours(6, 0, 0, 0);

				let DateEndNew = new Date(DateEnd);
				DateEndNew.setDate(DateEndNew.getDate() + 1);
				//DateEndNew.setHours(6, 0, 0, 0);
				//console.log('DateStartNew:', DateStartNew.format('yyyy-mm-dd hh:MM:ss'));
				
				let TypePayments = 3;
				TArray.push(JSON.stringify(new Struct_Table("Select", "metrics", "ReportSummaOnTypePaymentsFromCashBox", -1, 99999, 0, [Parameter.ReportSaleNew, Point, DateStartNew.format('yyyy-mm-dd 06:00:00'), DateEndNew.format('yyyy-mm-dd 06:00:00'), TypePayments])));
				TArray.push(JSON.stringify(new Struct_Table("Select", "metrics", "ReportSaleNewByInterval", -1, 99999, 0, [Parameter.ReportSaleNew, Point, DateStartNew, DateEndNew])));
				Link_action_obj.Select('ReportSaleNew', TArray);
				
				break;
			}
			case 'Rashod': {
				let TArray = [];
					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ReportByInterval", Parameter.Rashod, 99999, 0, [Parameter.Rashod, Sklad, DateStart, DateEnd])));
					Link_action_obj.Select('ReportPrihodRashod', TArray);
				break;
			}
			case 'Prixod': {
				let TArray = [];
					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ReportByInterval", Parameter.Prixod, 99999, 0, [Parameter.Prixod, Sklad, DateStart, DateEnd])));
					Link_action_obj.Select('ReportSale', TArray);
				break;
			}
			case 'Spisanie': {
				let TArray = [];
					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ReportByInterval", Parameter.Spisanie, 99999, 0, [Parameter.Spisanie, Sklad, DateStart, DateEnd])));
					Link_action_obj.Select('ReportPrihodRashod', TArray);
				break;
			}
			case 'Courier': {
				let TArray = [];
					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ReportCourierByInterval", Parameter.Courier, 99999, 0, [Parameter.Courier, Point, DateStart, DateEnd])));
					Link_action_obj.Select('ReportCourier', TArray);
				break;
			}
			case 'Operator': {
				let TArray = [];
					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ReportOperatorByInterval", Parameter.Operator, 99999, 0, [Parameter.Operator, DateStart, DateEnd])));
					Link_action_obj.Select('ReportOperator',TArray);
				break;
			}
			case 'Cashbox': {  //Кассовый отчёт
				$('#ReportSert').removeClass('hidden');
				$('#Report_Text_Sale').text('Касса:');
				let TArray = [];
					TArray.push( JSON.stringify(new Struct_Table("Select", "metrics", "ReportCashboxByInterval", Parameter.Cashbox, 99999, 0, [Parameter.Cashbox, Point, DateStart, DateEnd])));
					Link_action_obj.Select('ReportCashbox',TArray);
				//$('#data_dop_html').html("");
				break;
			}
			default:
				break;
		}
	});


});

function Struct_Table( Query, Table, TypeParameter, ParameterQuery, Limit, Offset, Values ) {
    this.Query = Query;
    this.Table = Table;
    this.TypeParameter = TypeParameter;
    this.ParameterQuery = ParameterQuery;
    this.Limit = Limit;
    this.Offset = Offset;
    this.Values = Values;
}

function Link_action() {}

Link_action.prototype.Select = function(Action, TableArray){
    console.log("Action:", Action, "TableArray:", TableArray);

    $.ajax({
        type: "POST",
        cache: false,
        async: false,
        data: ({"Tables": TableArray}),
        url: "/Common/Select",
        success: function(res) {
            console.log('Result: ', res);
            if (Error(String(res["Error"])) == true ) {
                return false;
            };

			$('#up-right-right').html('');

            switch (Action) {
                case 'ready': {
                    if (res['metrics_mod'] != null ) {
                        Modes = res['metrics_mod'];
                        Modes.splice(0,1);
                        groupChains_Modes();
                        viewChains_Modes();
                    }


                    if (res['metrics_type'] != null ) {

                        Types = res['metrics_type'];

                        //parseChains(); // распарсить Types[] в цепочки Chains[]
                        groupChains(); // группирую
                        viewChains();  // отобразить цепочки в HTML
                    } /////////////


                    if (res['metrics_link_type_and_mod_or_names'] != null ) {
                        ModeType = res['metrics_link_type_and_mod_or_names'];
                        viewModeType();
                    }

					if (res['metrics_dop_data.LoadHierarchy'] != null) {
						let MDD = res['metrics_dop_data.LoadHierarchy'];
						//Config.Metrics_hierarchy.Metrics_hierarchy_option = "<option></option>"
						//'<option></option>'
						//Config.Metrics_hierarchy.Metrics_hierarchy = [];
						Config.Metrics_hierarchy.Metrics_hierarchy_option = '<option data-id="-" data-hash="-"> Сеть ? </option>';
						for (let i=0;i<MDD.length;i++) {
							//Config.Metrics_hierarchy.Metrics_hierarchy[MDD[i].Parent_ID]=res['metrics_dop_data'];
							Config.Metrics_hierarchy.Metrics_hierarchy_option += '<option data-id="'+MDD[i].ID+'" data-hash="'+MDD[i].Hash+'">'+MDD[i].Name+'</option>'
						}
						//$('#TabChain').attr('data-id-activ', res['metrics_dop_data'][0].Hash);
						$('#TabSelectChain').append(Config.Metrics_hierarchy.Metrics_hierarchy_option);
						//let TArray=[];
						//TArray.push( JSON.stringify(new Struct_Table("Select", "metrics_dop_data", "LoadHierarchy", 99999, 0, [res['metrics_dop_data'][0].Hash])));
						//Link_action_obj.Select('dop', TArray);
						//$('#TabSelectFranc').append()
					}
                	break;
                }
				case 'org': {
					if (res['metrics_dop_data.LoadHierarchy'] != 'null') {
						let MDD = res['metrics_dop_data.LoadHierarchy'];
						Config.Metrics_hierarchy.Metrics_hierarchy_option = '<option data-id="-" data-hash="-"> Организация ? </option>';

						for (let i=0;i<MDD.length;i++) {
							Config.Metrics_hierarchy.Metrics_hierarchy_option += '<option data-id="'+MDD[i].ID+'" data-hash="'+MDD[i].Hash+'">'+MDD[i].Name+'</option>'
					  	}
						$('#TabSelectOrg').find('option').remove().end().append(Config.Metrics_hierarchy.Metrics_hierarchy_option);
					}
					break;
				}
				case 'point': {
					if (res['metrics_dop_data.LoadHierarchy'] != 'null') {
						let MDD = res['metrics_dop_data.LoadHierarchy'];
						Config.Metrics_hierarchy.Metrics_hierarchy_option = '<option data-id="-" data-hash="-"> Точка ? </option>';

						for (let i=0;i<MDD.length;i++) {
							Config.Metrics_hierarchy.Metrics_hierarchy_option += '<option data-id="'+MDD[i].ID+'" data-hash="'+MDD[i].Hash+'">'+MDD[i].Name+'</option>'
					  	}
						$('#TabSelectPoint').find('option').remove().end().append(Config.Metrics_hierarchy.Metrics_hierarchy_option);
					}
					break;
				}
				case 'sklad': {
					let count_refusals = 0,
						ml = res['metrics_dop_data.LoadHierarchy'],
						mp = res['metrics.ParametersByInterval.'+Parameter.AverageOrderAmountPerDay],
						mptnoo = res['metrics.ParametersByInterval.'+Parameter.TheNumberOfOrders],
						mptnorwwo= res['metrics.ParametersByInterval.'+Parameter.TheNumberOfRefusalsWithWriteOffs],
						mptnofwc = res['metrics.ParametersByInterval.'+Parameter.TheNumberOfFailuresWithoutCharge],
						link_count_refusals = $('#point_count_refusals'),
						link_middle_count = $('#point_middle_price'),
						link_order_count = $('#point_order_count'),
						link_write_offs = $('#point_write_offs'),
						link_without_cancellation = $('#point_without_cancellation');
					link_count_refusals.text(0);
					link_middle_count.text(0);
					link_order_count.text(0);
					link_write_offs.text(0);
					link_without_cancellation.text(0);
					if (ml != null) {
						if (ml.length == 0) {
							Error("Не получен ID склада");
							return false;
						}
						$('#TabPoint').attr('data-sklad', ml[0].Hash)
						console.log('Hash:', ml[0].Hash)
					}
					if (mp != null) {
						let MiddlePrice = 0;
						for (let i=0;i<mp.length;i++) {
							MiddlePrice += mp[i].Value;
						}
						link_middle_count.text( Math.round(MiddlePrice/mp.length,0) );
						console.log('MiddlePrice:',MiddlePrice)
					}
					if (mptnoo != null) {
						let OrderCount = 0;
						for (let i=0;i<mptnoo.length;i++) {
							OrderCount += mptnoo[i].Value;
						}
						link_order_count.text(OrderCount);
						console.log('OrderCount mptnoo:',OrderCount)
					}
					if (mptnorwwo != null) {
						let OrderCount = 0;
						for (let i=0;i<mptnorwwo.length;i++) {
							OrderCount += mptnorwwo[i].Value;
						}
						count_refusals += OrderCount
						link_write_offs.text(OrderCount);
						console.log('OrderCount mptnorwwo:',OrderCount)
					}
					if (mptnofwc != null) {
						let OrderCount = 0;
						for (let i=0;i<mptnofwc.length;i++) {
							OrderCount += mptnofwc[i].Value;
						}
						count_refusals += OrderCount
						link_without_cancellation.text(OrderCount);
						console.log('OrderCount mptnofwc:',OrderCount)
					}
					link_count_refusals.text(count_refusals)
					console.log('count_refusals:',count_refusals)
					break;
				}
				case 'ReportSale': {
					let m = res['metrics.ReportSaleByInterval'];
					if (m == null) {
						m = res['metrics.ReportSaleByInterval'];
						if (m == null) {
							return false;
						}
					}
					
					$('#table_report thead tr').append('<th>ID</th><th>Имя</th><th>Количество</th><th>Сумма</th><th>Себестоимость</th><th>Наценка</th>');
					//console.log(m.length);
					let sum=0, realfoodcost=0,
						newMap = new Map();
				
					for (let i=0;i<m.length;i++) {
						$('#table_report tbody').append(
							'<tr>'+
								'<td data-id="'+m[i].ID+'" data-price="'+m[i].Price_id+'">'+(i+1)+'</td>'+
								'<td>'+m[i].Name+'</td>'+
								'<td>'+m[i].Count+'</td>'+
								'<td>'+(m[i].Price).toFixed(2)+'</td>'+
								'<td>'+(m[i].Real_food_cost).toFixed(2)+'</td>'+
								'<td>' + (m[i].Real_food_cost != 0 ? ((m[i].Price - m[i].Real_food_cost) / m[i].Real_food_cost * 100).toFixed(2) + '%' : '-') + '</td>'+
							'</tr>'
						);
						ResizeTable();
						
						//console.log("newMap:",newMap);
						if ( newMap.has(m[i].Type_name) ) {
							let mas = newMap.get(m[i].Type_name);
								mas.push(m[i]);
								newMap.set(m[i].Type_name, mas)
						} else {
							newMap.set(m[i].Type_name, [m[i]])
						}
						sum+=Number(m[i].Price);
						realfoodcost += Number(m[i].Real_food_cost)
					}
					//console.log("newMap:", newMap)

					// Сертификаты из CashBox
					let len_sert = 0;
					
					let cb = res['metrics.ReportCashboxByInterval'];
					if (cb == null) {
						cb = res['metrics.ReportCashboxByInterval'];
						if (cb == null) {
							return false;
						}
					}
					for (let i=0; i<cb.length; i++) {
						if (cb[i].Type_payments == 3) {
							len_sert+=cb[i].Cash;
						}
					}
					console.log("sert: ", len_sert)
					//---

					sum -= len_sert;
					$('#sum_orders').text(sum.toFixed(2) + " руб.");
					$('#sum_realfoodcost').text(realfoodcost.toFixed(2) + " руб.");

					let div = '';
					newMap.forEach(function(elements, key) {
						let count = 0, summ = 0;
						//console.log('elements:',elements);
						for (let k in elements) {
							count += elements[k].Count;
							summ += elements[k].Price;
						}
						div += '<div class="col-md-6"><span>'+key+'</span>: <span>'+ count+'</span> на ' + summ.toFixed(2) + ' руб.</div>';
					}); 
					$('#data_dop_html').html(div);
	
					break;
				}
				case 'ReportSaleNew': {
//					if (!res['metrics.ReportSaleNewByInterval']) {
//						return false;	
//					}
					let m_ReportSaleNewByInterval = res['metrics.ReportSaleNewByInterval'];
					//console.log('m_ReportSaleNewByInterval:', m_ReportSaleNewByInterval);
					if (m_ReportSaleNewByInterval == null) {
						return false;
					}

					$('#table_report thead tr').append('<th>#</th><th>Имя</th><th>Количество</th><th>Сумма</th><th>Себестоимость</th><th>Наценка</th>');
					let sum_orders = 0, sum_realfoodcost = 0, sum_sert = 0,
						newMap = new Map();
				
					for (let i=0; i<m_ReportSaleNewByInterval.length; i++) {
						let price_markup = (m_ReportSaleNewByInterval[i].Real_food_cost != 0 ? ((m_ReportSaleNewByInterval[i].Price - m_ReportSaleNewByInterval[i].Real_food_cost) / m_ReportSaleNewByInterval[i].Real_food_cost * 100).toFixed(0) + '%' : '-');
						$('#table_report tbody').append(
							'<tr>'+
								'<td data-price="' + m_ReportSaleNewByInterval[i].Price_id+'">' + (i+1) + '</td>'+
								'<td>' + m_ReportSaleNewByInterval[i].Name + '</td>'+
								'<td>' + m_ReportSaleNewByInterval[i].Count + '</td>'+
								'<td>' + (m_ReportSaleNewByInterval[i].Price).toFixed(2) + '</td>'+
								'<td>' + (m_ReportSaleNewByInterval[i].Real_food_cost).toFixed(2) + '</td>'+
								'<td>' + price_markup + '</td>'+
							'</tr>'
						);
						ResizeTable();
						
						//console.log("newMap:",newMap);
						if (newMap.has(m_ReportSaleNewByInterval[i].Type_name) ) {
							let mas = newMap.get(m_ReportSaleNewByInterval[i].Type_name);
								mas.push(m_ReportSaleNewByInterval[i]);
								newMap.set(m_ReportSaleNewByInterval[i].Type_name, mas)
						} else {
							newMap.set(m_ReportSaleNewByInterval[i].Type_name, [m_ReportSaleNewByInterval[i]])
						}

						sum_orders += Number(m_ReportSaleNewByInterval[i].Price);
						sum_realfoodcost += Number(m_ReportSaleNewByInterval[i].Real_food_cost)
					}
					//console.log("newMap:", newMap)

					// Сертификаты из ReportSummaOnTypePayments
					let m_ReportSummaOnTypePayments = res['metrics.ReportSummaOnTypePaymentsFromCashBox'];
					if (m_ReportSummaOnTypePayments !== null) {
						console.log("m_ReportSummaOnTypePayments: ", m_ReportSummaOnTypePayments)
						sum_sert = m_ReportSummaOnTypePayments[0];
						console.log("sum_sert:", sum_sert);
					}
					//---

					sum_orders -= sum_sert;
					$('#sum_orders').html(sum_orders.toFixed(2) + " руб. <br>(Сертификаты: " + sum_sert.toFixed(2) + " руб.)");
					$('#sum_realfoodcost').text(sum_realfoodcost.toFixed(2) + " руб.");

					let div = '';
					newMap.forEach(function(elements, key) {
						let count = 0, summ = 0;
						for (let k in elements) {
							count += elements[k].Count;
							summ += elements[k].Price;
						}
						div += '<div class="col-md-3"><span>'+key+'</span>: <span>'+ count+'</span> на ' + summ.toFixed(2) + ' руб.</div>';
					}); 
					$('#data_dop_html').html(div);
	
					break;
				}

				case 'ReportPrihodRashod': {
					let m = res['metrics.ReportByInterval'];
					if (m == null) {
						m = res['metrics.ReportByInterval'];
						if (m == null) {
							return false;
						}
					}
					$('#table_report thead tr').append('<th class="report_sale_id">ID</th><th class="report_sale_name">Имя</th><th class="report_sale_count">Количество</th><th class="report_sale_price">Цена</th>');
					//console.log(m.length);
					let sum=0,
						newMap = new Map();
				
					for (let i=0;i<m.length;i++) {
						$('#table_report tbody').append(
							'<tr>'+
								'<td class="col-xs-3" id="table_id" data-id="'+m[i].ID+'" data-price="'+m[i].Price_id+'">'+(i+1)+'</td>'+
								'<td class="col-xs-4" id="table_name">'+m[i].Name+'</td>'+
								'<td class="col-xs-3" id="table_count">'+m[i].Count+'</td>'+
								'<td class="col-xs-2" id="table_price">'+m[i].Price+'</td>'+
							'</tr>'
						);
						
						//console.log("newMap:",newMap);
						if ( newMap.has(m[i].Type_name) ) {
							let mas = newMap.get(m[i].Type_name);
								mas.push(m[i]);
								newMap.set(m[i].Type_name, mas)
						} else {
							newMap.set(m[i].Type_name, [m[i]])
						}
						sum+=Number(m[i].Price);
					}
					//console.log("newMap:", newMap)
					$('#sum_orders').text(sum.toFixed(2));

					let div = '';
					newMap.forEach(function(elements, key) {
						let count = 0, summ = 0;
						for (let k in elements) {
							count += elements[k].Count;
							summ += elements[k].Price;
						}
						div += '<div class="col-md-6"><span>'+key+'</span>: <span>'+ count+'</span> на ' + summ.toFixed(2) + ' руб.</div>';
					}); 
					$('#data_dop_html').html(div);
	
					break;
				}
				case 'ReportCourier': {
					let m = res['metrics.ReportCourierByInterval'];
					if (m == null) {
						m = res['metrics.ReportCourierByInterval'];
						if (m == null) {
							return false;
						}
					}
					$('#table_report thead tr').append('<th class="report_courier_name">Имя</th><th class="report_courier_count">Количество</th><th class="report_courier_array">Массив ID заказов</th>');
					//console.log(m.length);
					for (let i=0;i<m.length;i++) {
						$('#table_report tbody').append(
							'<tr>'+
								//'<td class="col-xs-3" id="table_id" data-id="'+m[i].ID+'" data-price="'+m[i].Price_id+'">'+(i+1)+'</td>'+
								'<td class="col-xs-4" id="table_hash" tr-data-hash="'+m[i].Hash+'">'+Config.Users.Users[m[i].Hash].Name+' ('+Config.Users.Users[m[i].Hash].PhoneNumber+')'+'</td>'+
								'<td class="col-xs-3" id="table_count">'+m[i].Count+'</td>'+
								'<td  style="width:41.7%; max-width:500px;" id="table_array">'+m[i].ArrayOrdersID+'</td>'+
							'</tr>'
						);
					}
					break;
				}
				case 'ReportOperator': {
					let m = res['metrics.ReportOperatorByInterval'];
					if (m == null) {
						m = res['metrics.ReportOperatorByInterval'];
						if (m == null) {
							return false;
						}
					}
					$('#table_report thead tr').append('<th class="report_operator_name">Имя</th><th class="report_operator_count">Количество</th>');
					//console.log(m.length);
					for (let i=0;i<m.length;i++) {
						$('#table_report tbody').append(
							'<tr>'+
								//'<td class="col-xs-3" id="table_id" data-id="'+m[i].ID+'" data-price="'+m[i].Price_id+'">'+(i+1)+'</td>'+
								'<td class="col-xs-6" id="table_hash" tr-data-hash="'+m[i].Hash+'">'+Config.Users.Users[m[i].Hash].Name+' ('+Config.Users.Users[m[i].Hash].PhoneNumber+')'+'</td>'+
								'<td class="col-xs-6" id="table_count">'+m[i].Count+'</td>'+
							'</tr>'
						);
					}
					break;
				}
				case 'ReportCashbox': {
					//CashRegister, Action_timeStr, UserHash, Info, Type_payments, Cash, Date_preorder
					
					$('#data_dop_html').html('');
					
					let m = res['metrics.ReportCashboxByInterval'];
					if (m == null) {
						m = res['metrics.ReportCashboxByInterval'];
						if (m == null) {
							return false;
						}
					}
					$('#table_report thead tr').append('<th class="report_cashbox_number_shift">№ смены</th>\
					<th class="report_cashbox_time">Время</th>\
					<th class="report_cashbox_name">Имя</th>\
					<th class="report_cashbox_info">Информация</th>\
					<th class="report_cashbox_payment">Тип оплаты</th>\
					<th class="report_cashbox_summ">Сумма</th>\
					<th class="report_cashbox_preorder">Предзаказ</th>');
					
					//console.log(m.length);
					let sum=0, len_sert = 0, count_sert = 0, cash_count_plus = 0, cash_count_minus = 0;
					
					let down_arr = [];
					
					for (let i=0;i<m.length;i++) {
						$('#table_report tbody').append(
							'<tr>'+
								'<td class="col-xs-2" id="table_id" cashcegister-id="'+m[i].CashRegister+'">'+m[i].CashRegister+'</td>'+
								'<td class="col-xs-3" id="table_count">'+m[i].Action_time.split('.')[0].replace("T"," ")+'</td>'+
								'<td class="col-xs-4" id="table_hash">'+Config.Users.Users[m[i].UserHash].Name+' ('+Config.Users.Users[m[i].UserHash].PhoneNumber+')'+'</td>'+
								'<td class="col-xs-2" id="table_array">'+m[i].Info+'</td>'+
								'<td class="col-xs-2" id="table_array">'+m[i].Type_payments+'</td>'+
								'<td class="col-xs-2" id="table_array">'+m[i].Cash+'</td>'+
								'<td class="col-xs-2" id="table_array">' + (m[i].Date_preorder == '0001-01-01T00:00:00Z' ? '-' : m[i].Date_preorder.replace("T"," ").replace("Z","")) + '</td>'+
							'</tr>'
						);
						//console.log("m[i].Type_payments:", m[i].Type_payments);
						
						if (m[i].Type_payments == 3) {
							len_sert+=m[i].Cash;
							count_sert++;
						} else {
							if ( Number(m[i].Cash) >= 0 ) {
								cash_count_plus += Number(m[i].Cash);
							} else {
								cash_count_minus += -Number(m[i].Cash);
							}
							sum+=Number(m[i].Cash);
						}
						
						// массив для блока под таблицей
						let dp = new Date(m[i].Date_preorder).toISOString().split('T')[0];
						let at = new Date(m[i].Action_time).toISOString().split('T')[0];
						console.log("date: ", dp);
						
						if (m[i].Date_preorder != '0001-01-01T00:00:00Z' && dp != at)
						{
							if (!(dp in down_arr)) {
								down_arr[dp] = 0;	
							}
							down_arr[dp] += m[i].Cash;	
						}
						//---
					}
	
					$('#sum_orders').text(sum.toFixed(2));
					$('#ReportSert #sert_orders').text(len_sert.toFixed(2));
					$('#ReportSert #sert_count_orders').text(count_sert.toFixed(0));
					$('#ReportSert #cash_count_plus').text(cash_count_plus.toFixed(2));
					$('#ReportSert #cash_count_minus').text(cash_count_minus.toFixed(2));
					
					// блок под таблицей	
					let div = '';

					for(let i in down_arr) {
						console.log(i + ' - '  + down_arr[i]);
						div += '<div class="col-md-12"><span>' + i + '</span>: <span>' + down_arr[i] + ' руб.</span></div>';	
					}

					if(div != '') {
						div = '<div class="col-md-12">Предзаказы:</div>' + div;
						$('#up-right-right').html(div);	
					}
					//---
					
					break;
				}
				case 'CancellationOrder': {
					let div =  $('<div>  <table class="table"><thead><th>Заказ</th><th>Время заказа</th><th>Время отмены</th><th>Кто отменил</th><th>Причина</th><th>Списание</th></thead><tbody> </tbody></table> </div>');
					
					for (let key in res['CancellationOrder']) {
							let el = res['CancellationOrder'][key];
							let canc = 'без списания';
							if (el.Status_id == 15) {
								canc = $('<button type="button" class="btn btn-warning" >со списанием</button>');
								canc.click(ShowMoreInfoSoSpisaniem.bind(null,el));
							}
							let row = $('<tr><td>'+el.Order_id+
									'</td><td>'+el.Order_time.split(".")[0].split("T")[1]+
									'</td><td>'+el.Cancellation_time.split(".")[0].split("T")[1]+
									'</td><td>'+el.User_name+
									'</td><td>'+el.Cancellation_note+
									'</td ><td class="actiontd">' +
									'</td></tr>');
							row.find(".actiontd").append(canc);
							div.find("tbody").append (row);
							
						}
						
					$('#modal_title_canc').text('Отменённые заказы по всем точкам на '+ $('#filter_date_start').val()  + " всего:"+res['CancellationOrder'].length);
					$('#canc').html(div)
					
		
				}               
				default:
                  	Error("NOT_ACTION_SELECT_JS");
                  	break;
            }
        },
        complete: function() {}
    });
}

Link_action.prototype.Action = function(Action, TableArray){
    console.log("Action:", Action, "TableArray:", TableArray);
    $.ajax({
        type: "POST",
        cache: false,
        async: false,
        data: ({"Tables": TableArray}),
        url: "/Common/Action",
        success: function(res) {
            console.log(res);
            if (Error(String(res["Error"])) == true ) {
                return false;
            };
            switch (Action) {
                case 'ready':
                {
                    if (res['metrics_mod'] != null ) {
                        let TableArray = [];
                        TableArray.push( JSON.stringify(new Struct_Table("Select", "metrics_mod", "", -1, 99999, 0)));
                        Link_action_obj.Select('ready',TableArray);
                    }
                    if (res['metrics_type'] != null ) {
                        let TableArray = [];
                        TableArray.push( JSON.stringify(new Struct_Table("Select", "metrics_type", "", -1, 99999, 0)));
                        Link_action_obj.Select('ready',TableArray);
                    }
                    if (res['metrics_link_type_and_mod'] != null ) {
                        let TableArray = [];
                        TableArray.push( JSON.stringify(new Struct_Table("Select", "metrics_link_type_and_mod_or_names", "", -1, 99999, 0)));
                        Link_action_obj.Select('ready',TableArray);
                    }
                    if(res["Error"]!= null)
                    {
                        if(res["Error"].indexOf("violates foreign key constraint")>0)
                        {
                            alert("Невозможно удаление - к этой записи привязаны другие записи.");
                        }

                    }
                }
                    break;
                default:
                    Error("NOT_ACTION_SELECT_JS");
                    break;
            }
        },
        complete: function() {}
    });
}

function Users(Hash, Name, RoleHash, PhoneNumber){
	this.Hash = Hash;
	this.Name = Name;
	this.RoleHash = RoleHash;
	this.PhoneNumber = PhoneNumber;
}

Link_action.prototype.Config = function(TableArray){
	$.ajax({
	    type: "GET",
		cache: false,
 		async: false,
	    url: "/Common/Config",
		data: ({"Tables": TableArray}),
	    success: function(res) {
			console.log(res);
			if (Error(res["Error"]) == true) {
				return;
			}

			if (res['Users']!=null){
				Config.Users.Users = [];
				Config.Users.UsersOption = '<option></option>'
				for (let key in res['Users']) {
					let Obj = new Users(res['Users'][key].Hash, res['Users'][key].Name, res['Users'][key].RoleHash, res['Users'][key].PhoneNumber);
					Config.Users.Users[res['Users'][key].Hash] = Obj;
					Config.Users.UsersOption += '<option data-hash="'+res['Users'][key].Hash+'" value="'+res['Users'][key].Name+'" data-rolehash="'+res['Users'][key].RoleHash+'">'+res['Users'][key].Name+' ('+res['Users'][key].PhoneNumber+')'+'</option>';
				}
			}


			if (res['CancellationOfOrder']!=null){
				
			}
			//console.log("Config.Users:",Config.Users);
	  	},
		complete: function() {}
	});
}


$(document).on('click',".delete",function() {
    //надо ещё удалять из БД
    console.log(this.id);// id элемента, надо удалить всех потомков!

    let TableArray = [];

    if(this.value == "type")
    {
        TableArray.push( JSON.stringify(new Struct_Table("Delete", "metrics_type", "Id", 99999, -1, 0, [+this.id])));
        Link_action_obj.Action('ready',TableArray);
        for(let i = 0; i<Types.length;i++)
        {
            if(this.id == Types[i].ID)
            {
                console.log('удаление предка:',i);
                Types.splice(i,1);
                continue;
            }
            if(this.id == Types[i].Parent_ID)
            {
                console.log('удаление потомка:',i);
                Types.splice(i,1);
            }
            if(Types[i].children)
            {
                for(let j = 0; j<Types[i].children.length; j++)
                {
                    if(this.id == Types[i].children[j].ID)
                    {
                        Types[i].children.splice(j,1);
                        break;
                    }
                }
            }
        }
    }

    else if(this.value == "mode")
    {
        TableArray.push( JSON.stringify(new Struct_Table("Delete", "metrics_mod", "Id", -1, 99999, 0, [+this.id])));
        Link_action_obj.Action('ready',TableArray);
        for(let i = 0; i<Modes.length;i++)
        {
            if(this.id == Modes[i].ID)
            {
                console.log('удаление предка:',i);
                Modes.splice(i,1);
                continue;
            }
            if(this.id == Modes[i].Parent_ID)
            {
                console.log('удаление потомка:',i);
                Modes.splice(i,1);
            }
            if(Modes[i].children)
            {
                for(let j = 0; j<Modes[i].children.length; j++)
                {
                    if(this.id == Modes[i].children[j].ID)
                    {
                        Modes[i].children.splice(j,1);
                        break;
                    }
                }
            }
        }
    }



    //$(this.parentNode).remove();

});
$(document).on('click',".save",function() {

    let newName = $("#ex1").val();
    if(newName == "")
    {
        alert("Введите название");
        return;
    }

    let TableArray = [];
    if(this.value == "type")
        TableArray.push( JSON.stringify(new Struct_Table("Insert", "metrics_type", "", -1, 99999, 0, [+this.id,newName])));
    else if(this.value == "mode")
        TableArray.push( JSON.stringify(new Struct_Table("Insert", "metrics_mod", "", -1, 99999, 0, [+this.id,newName])));

    Link_action_obj.Action('ready',TableArray);

    ///ajax save row
    $("#myModal").modal('hide'); //ModalEditInfo

    console.log('Сохранение cтроки id:',this.id);
});
$(document).on('click',".move",function() {
    let newParent = undefined;
    let name = "error";
    let tablename = "";
    if(this.value == "type")
    {
        newParent = getSelectedRadioId("type");
        name = getNameType(this.id);
        tablename = "metrics_type";
    }
    else if(this.value == "mode"){
         newParent = getSelectedRadioId("mode");
        name = getName_Modes(this.id);
        tablename = "metrics_mod";
    }
    if(newParent == undefined)
    {
        alert("Не выбран мод или тип");
        return;
    }
    //мод выбран, дальше формируем строку

    let obj = {ID: this.id,Name: name, Parent_ID:newParent};
    let TableArray = [];
        TableArray.push( JSON.stringify(new Struct_Table("Update", tablename, "Id", -1, 99999, 0, [+obj.ID,+obj.Parent_ID,obj.Name])));

    Link_action_obj.Action('ready',TableArray);
}); //смена родителя



function changeActiveRow(radio) {
    $(radio.parentNode.parentNode).addClass('active').siblings().removeClass('active');
};


function getSelectedRadioId(value) {
    let inp = document.getElementsByName(value);
    for (let i = 0; i < inp.length; i++) {
        if (inp[i].type == "radio" && inp[i].checked) {
            return inp[i].value;
        }
    }
}
function uncheckRadio(value) {
    let inp = document.getElementsByName(value);
    for (let i = 0; i < inp.length; i++) {
        if (inp[i].type == "radio" && inp[i].checked) {
            inp[i].checked = false;
        }
    }
}
function hidemodalsospisaniem(){
	$('#myModalSoSpisaniem').modal("hide");
}
//
function ShowMoreInfoSoSpisaniem(arrayElements){
	console.log(arrayElements);
	let div =  $('<div>  <table class="table"><thead><th>№</th><th>Название</th></thead><tbody> </tbody></table> </div>');
					
					for (let key in arrayElements.Comp) {
							let el = arrayElements.Comp[key];
							
							let row = $('<tr><td>'+key+
									'</td><td>'+el.Price_name+
									'</td></tr>');
							div.find("tbody").append (row);
						}
						
					$('#modal_title_sospisaniem').text('Списанные продукты. Всего: '+arrayElements.Comp.length);
					$('#spisano').html(div)
	$('#myModalSoSpisaniem').modal("show");
}

function tree_toggle(event) {
    event = event || window.event
    var clickedElem = event.target || event.srcElement

    if (!hasClass(clickedElem, 'Expand')) {
        return // клик не там
    }

    // Node, на который кликнули
    var node = clickedElem.parentNode
    if (hasClass(node, 'ExpandLeaf')) {
        return // клик на листе
    }

    // определить новый класс для узла
    var newClass = hasClass(node, 'ExpandOpen') ? 'ExpandClosed' : 'ExpandOpen'
    // заменить текущий класс на newClass
    // регексп находит отдельно стоящий open|close и меняет на newClass
    var re =  /(^|\s)(ExpandOpen|ExpandClosed)(\s|$)/
    node.className = node.className.replace(re, '$1'+newClass+'$3')
}


function hasClass(elem, className) {
    return new RegExp("(^|\\s)"+className+"(\\s|$)").test(elem.className)
}


function GetDates(Format) {
	let T = new Date();
	let maps = {
		'dd': (T.getDate() < 10) ? ('0' + T.getDate()) : (T.getDate()),
		'mm': ((T.getMonth() + 1) < 10) ? ('0' + (T.getMonth() + 1)) : (T.getMonth() + 1),
		'gggg': T.getFullYear()
	};
	function replacer(str, p1, offset, s) {
		return maps[p1];
	}
	return Format.replace(/([a-z]{2,4})/g, replacer)
}


function GetFirstDates(Format) {
	let T = new Date();
	let maps = {
		'dd': '01',
		'mm': ((T.getMonth() + 1) < 10) ? ('0' + (T.getMonth() + 1)) : (T.getMonth() + 1),
		'gggg': T.getFullYear()
	};
	function replacer(str, p1, offset, s) {
		return maps[p1];
	}
	return Format.replace(/([a-z]{2,4})/g, replacer)
}


// Adjust the width of thead cells when window resizes
function ResizeTable() {
	// Change the selector if needed
	var $table = $('table.scroll'),
	    $bodyCells = $table.find('tbody tr:first').children(),
	    colWidth;
    // Get the tbody columns width array
    colWidth = $bodyCells.map(function() {
        return $(this).width();
    }).get();
    //console.log(colWidth);
    // Set the width of thead columns
    $table.find('thead tr').children().each(function(i, v) {
        $(v).width(colWidth[i]);
    });    
};

Date.prototype.format = function(format = 'yyyy-mm-dd') {
    const replaces = {
        yyyy: this.getFullYear(),
        mm: ('0'+(this.getMonth() + 1)).slice(-2),
        dd: ('0'+this.getDate()).slice(-2),
        hh: ('0'+this.getHours()).slice(-2),
        MM: ('0'+this.getMinutes()).slice(-2),
        ss: ('0'+this.getSeconds()).slice(-2)
    };
    let result = format;
    for(const replace in replaces){
        result = result.replace(replace,replaces[replace]);
    }
    return result;
};