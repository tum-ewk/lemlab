pragma solidity >=0.5.0 <0.7.5;
pragma experimental ABIEncoderV2;

import "./ClearingExAnte.sol";
import "./Sorting.sol";

contract Settlement {

    event logString(string arg);
    Lb.LemLib lib= new Lb.LemLib();		//instance of the contract LemLib(general library with useful functionalities)
	Sorting srt = new Sorting();		//instance of the contract Sorting(useful sorting functionalities)
	ClearingExAnte clearing;
	address clearing_add;

	// array of meter_reading_deltas per timestep, usually around 150 in length
	Lb.LemLib.meter_reading_delta[][672] meter_reading_deltas;
	// in solitidy, in the definition the order of indexes is inversed, so this is in reality a 672xnum_meters matrix
	// the number of meters is left uninitialized so they can be pushed and modified
	Lb.LemLib.energy_balancing[][672] energy_balances;		//need to be constant numbers, no variables
	Lb.LemLib.price_settlement[672] prices_settlement;
	Lb.LemLib.log_transaction[][672] logs_transaction;

	constructor(address clearing_ex_ante) public{
		// since the size of the data needs to be hard coded, we check it matches the lib contract
		require(lib.get_horizon()==energy_balances.length, "The horizon does not match the one specified in the Lib contract");
		clearing=ClearingExAnte(clearing_ex_ante);
		clearing_add=clearing_ex_ante;
		}
	// events used for debugging and printing
	event energy_added(uint ts);
	event wrong_number_meters(string log);

	function clear_data() public{
		delete meter_reading_deltas;
		delete energy_balances;
		delete prices_settlement;
		delete logs_transaction;
	}

	function get_clearing_add() public view returns(address){
		return clearing_add;
	}

	function clear_data_gas_limit(uint max_entries, uint sec_half) public {
		for(uint i = 0; i < max_entries; i++){
//			if(Settlement.meter_reading_deltas.length > 0){
//				Settlement.meter_reading_deltas.length--;
//			}
			if(i+sec_half < lib.get_horizon()){
				delete Settlement.energy_balances[i+sec_half];
				delete Settlement.meter_reading_deltas[i+sec_half];
				delete Settlement.prices_settlement[i+sec_half];
				delete Settlement.logs_transaction[i+sec_half];
			}
		}
	}

	// utility implementation for not changing of cyontract in python
	function get_horizon()public view returns(uint){
		return lib.get_horizon();
	}

	// not used anymore, left for utility
	function get_num_meters()public view returns(uint){
		return lib.get_num_meters();
	}

	// pushes the delta readings into the array
	function push_meter_readings_delta(Lb.LemLib.meter_reading_delta memory meter_delta) public {
		uint ts = lib.ts_delivery_to_index(meter_delta.ts_delivery);
		if( ts>671){		// safe check for debugging, technically, the ts_index should never pass 671
			emit energy_added(ts);	// emits an energy added event to catch on the tests
			ts=671;
		}
		meter_reading_deltas[ts].push(meter_delta);
	}

	// function to push the energy balance to the matrix of energies
	// The rows index is the ts_delivery
	function push_energy_balance(Lb.LemLib.energy_balancing memory e_balance) public {
		uint ts = lib.ts_delivery_to_index(e_balance.ts_delivery);
		if( ts>671){		// safe check for debugging, technically, the ts_index should never pass 671
			emit energy_added(ts);	// emits an energy added event to catch on the tests
			ts=671;
		}
		//energy_balances[ts][meter_id]=e_balance;
		energy_balances[ts].push(e_balance);
	}

	function get_meter_readings_delta() public view returns (Lb.LemLib.meter_reading_delta[] memory){
		uint count = 0;
		for(uint i = 0; i < lib.get_horizon(); i++){
			count += meter_reading_deltas[i].length;
		}
		// safe check, if there are no meter reading deltas we cannot create an array with length 0
		// so we create a single element array with ts_delivery equal to -1, this will be later
		// filtered out in the bc_connection python interface
		if(count == 0){
			Lb.LemLib.meter_reading_delta[] memory sample = new Lb.LemLib.meter_reading_delta[](1);
			sample[0].ts_delivery = uint(-1);
			return sample;
		}
		else{
			uint ind = 0;
			Lb.LemLib.meter_reading_delta[] memory results = new Lb.LemLib.meter_reading_delta[](count);
			for(uint i = 0; i < lib.get_horizon(); i++){
				for(uint j = 0; j < meter_reading_deltas[i].length; j++){
					results[ind] = meter_reading_deltas[i][j];
					ind++;
				}
			}
		return results;
		}
	}

	function get_meter_readings_delta_by_ts(uint ts) public view returns (Lb.LemLib.meter_reading_delta[] memory){
		uint index = lib.ts_delivery_to_index(ts);
		return meter_reading_deltas[index];
	}

	function get_id_meters() public view returns(Lb.LemLib.id_meter[] memory){
		return clearing.get_id_meters();
	}

	//function to return the energy balance of an specific timestep
	function get_energy_balance_by_ts(uint ts) public view returns(Lb.LemLib.energy_balancing[] memory){
		uint index = lib.ts_delivery_to_index(ts);
		return energy_balances[index];
	}

	// same function for getting all the energy balances
	function get_energy_balance_all() public view returns(Lb.LemLib.energy_balancing[] memory){
		uint count = 0;
		for(uint i = 0; i < lib.get_horizon(); i++){
			count += energy_balances[i].length;
		}
		// safe check, if there are no energy balances we cannot create an array with length 0
		// so we create a single element array with the ts_delivery equal to -1, this will be later
		// filtered out in the bc_connection python interface
		if(count == 0){
			Lb.LemLib.energy_balancing[] memory sample = new Lb.LemLib.energy_balancing[](1);
			sample[0].ts_delivery = uint(-1);
			return sample;
		}
		else{
			uint ind = 0;
			Lb.LemLib.energy_balancing[] memory results = new Lb.LemLib.energy_balancing[](count);
			for(uint i = 0; i < lib.get_horizon(); i++){
				for(uint j = 0; j < energy_balances[i].length; j++){
					results[ind] = energy_balances[i][j];
					ind++;
				}
			}
		return results;
		}
	}

	function get_prices_settlement_by_ts(uint ts) public view returns(Lb.LemLib.price_settlement memory){
		uint index = lib.ts_delivery_to_index(ts);
		return prices_settlement[index];
	}

	function get_prices_settlement() public view returns(Lb.LemLib.price_settlement[] memory){
		uint count = 0;
		for(uint i = 0; i < lib.get_horizon(); i++){
			if(prices_settlement[i].ts_delivery != 0){
				count++;
			}
		}
		// safe check, if there are no energy balances we cannot create an array with length 0
		// so we create a single element array with the ts_delivery equal to -1, this will be later
		// filtered out in the bc_connection python interface
		if(count==0){
			Lb.LemLib.price_settlement[] memory sample=new Lb.LemLib.price_settlement[](1);
			sample[0].ts_delivery=uint(-1);
			return sample;
		}
		else{
			uint ind=0;
			Lb.LemLib.price_settlement[] memory prices = new Lb.LemLib.price_settlement[](count);
			for(uint i=0; i<lib.get_horizon(); i++){
				if(prices_settlement[i].ts_delivery!=0){
					prices[ind]=prices_settlement[i];
					ind++;
				}
			}
		return prices;
		}
	}
	function get_logs_transactions_by_ts(uint ts) public view returns(Lb.LemLib.log_transaction[] memory){
		uint index = lib.ts_delivery_to_index(ts);
		return logs_transaction[index];
	}
	function get_logs_transactions() public view returns(Lb.LemLib.log_transaction[] memory){
		uint count=0;
		for(uint i=0; i<lib.get_horizon(); i++){
			if(logs_transaction[i].length>0){
				count+= logs_transaction[i].length;
			}
		}
		// safe check, if there are no energy balances we cannot create an array with length 0
		// so we create a single element array with the ts_delivery equal to -1, this will be later
		// filtered out in the bc_connection python interface
		if(count==0){
			Lb.LemLib.log_transaction[] memory sample=new Lb.LemLib.log_transaction[](1);
			sample[0].ts_delivery=uint(-1);
			return sample;
		}
		else{
			uint ind=0;
			Lb.LemLib.log_transaction[] memory logs = new Lb.LemLib.log_transaction[](count);
			for(uint i=0; i<lib.get_horizon(); i++){
				for(uint j=0; j<logs_transaction[i].length; j++){
					logs[ind]=logs_transaction[i][j];
					ind++;
				}
			}
		return logs;
		}
	}
	// we get the total market results, as the temp list is empty
	// this is just for utility to call it from the Settlement contract instance instead of the ClearingExAnte instance
	function get_market_results_total() public view returns(Lb.LemLib.market_result_total[] memory){
		return clearing.get_market_results_total();
	}

	// function to determine the changes in energy for a given list of time steps
	// the function calculates the change of energy for every meter inside a specific timestep
	// Finally, it pushes the results to a mapping according to the timestep
    function determine_balancing_energy(uint[] memory list_ts_delivery) public{
		if(list_ts_delivery.length==0){
			return;
		}
		for(uint i=0; i<list_ts_delivery.length; i++){
			Lb.LemLib.meter_reading_delta[] memory meters=lib.meters_delta_inside_ts_delivery(get_meter_readings_delta(), list_ts_delivery[i]);
			Lb.LemLib.market_result_total[] memory results=lib.market_results_inside_ts_delivery(get_market_results_total(), list_ts_delivery[i]);

			if(meters[0].ts_delivery<0 || results[0].ts_delivery<0){
				continue;	// this means that no meters or market_results were found for that ts
			}
			for(uint j=0; j<meters.length;j++){
				int current_actual_energy=int(meters[j].energy_out)-int(meters[j].energy_in);
				int current_market_energy=0;
				for(uint k=0; k<results.length;k++){
					if(lib.compareStrings(meters[j].id_meter, results[k].id_user_bid)){
						current_market_energy -=  int(results[k].qty_energy_traded);
					}
					else if(lib.compareStrings(meters[j].id_meter, results[k].id_user_offer)){
						current_market_energy += int(results[k].qty_energy_traded);
					}
				}
				current_actual_energy -= current_market_energy;
				Lb.LemLib.energy_balancing memory result_energy;
				result_energy.id_meter=meters[j].id_meter;
				result_energy.ts_delivery=list_ts_delivery[i];
				// in a similar way to python´s decompose float function, we store the difference in energy if positive or negative
				if(current_actual_energy>=0){
					result_energy.energy_balancing_positive=uint(current_actual_energy);
					result_energy.energy_balancing_negative=0;
				}
				else{
					result_energy.energy_balancing_positive=0;
					result_energy.energy_balancing_negative=uint(-current_actual_energy);
				}
				Settlement.push_energy_balance(result_energy);
			}

		}
	}
	// function to set the prices settlement given a list of ts_deliveries and the ints of the prices
	// the prices are expected to be in an already sigma format
	function set_prices_settlement_custom(uint[] memory list_ts_delivery, uint price_bal_pos, uint price_bal_neg,
											uint price_lev_pos, uint price_lev_neg) public{
		if(list_ts_delivery.length==0){
			return;
		}
		Lb.LemLib.price_settlement memory price;
		price.price_energy_balancing_positive=price_bal_pos;
		price.price_energy_balancing_negative=price_bal_neg;
		price.price_energy_levies_positive=price_lev_pos;
		price.price_energy_levies_negative=price_lev_neg;
		for(uint i=0; i<list_ts_delivery.length; i++){
			price.ts_delivery=list_ts_delivery[i];
			uint ts = lib.ts_delivery_to_index(list_ts_delivery[i]);
			prices_settlement[ts]=price;
		}

	}
	// same function but set different prices for each timestep
	function set_prices_settlement_custom_list(uint[] memory list_ts_delivery, uint[] memory price_bal_pos,
												uint[] memory price_bal_neg, uint[] memory price_lev_pos,
												uint[] memory price_lev_neg) public{
		if(list_ts_delivery.length==0){
			return;
		}
		// the current python implemetation already checks if all the list have the same length
		Lb.LemLib.price_settlement memory price;
		for(uint i=0; i<list_ts_delivery.length; i++){
			price.ts_delivery=list_ts_delivery[i];
			price.price_energy_balancing_positive=price_bal_pos[i];
			price.price_energy_balancing_negative=price_bal_neg[i];
			price.price_energy_levies_positive=price_lev_pos[i];
			price.price_energy_levies_negative=price_lev_neg[i];
			uint ts = lib.ts_delivery_to_index(list_ts_delivery[i]);
			prices_settlement[ts]=price;
		}
	}
	// function to set the prices settlement given a list of ts_deliveries, the prices are set to the default
	// values right now
	function set_prices_settlement(uint[] memory list_ts_delivery) public{
		if(list_ts_delivery.length==0){
			return;
		}
		Lb.LemLib.price_settlement memory price;
		price.price_energy_balancing_positive=15e4;
		price.price_energy_balancing_negative=15e4;
		price.price_energy_levies_positive=0;
		price.price_energy_levies_negative=18e4;
		for(uint i=0; i<list_ts_delivery.length; i++){
			price.ts_delivery=list_ts_delivery[i];
			uint ts = lib.ts_delivery_to_index(list_ts_delivery[i]);
			prices_settlement[ts]=price;
		}
	}

	function update_balance_balancing_costs(uint[] memory list_ts_delivery, uint ts_now, string memory supplier) public{
		if(list_ts_delivery.length==0){
			return;
		}

		for(uint i=0; i<list_ts_delivery.length; i++){
			Lb.LemLib.price_settlement memory settlement_price = get_prices_settlement_by_ts(list_ts_delivery[i]);
			Lb.LemLib.energy_balancing[] memory energy_bal = get_energy_balance_by_ts(list_ts_delivery[i]);
			uint index = lib.ts_delivery_to_index(list_ts_delivery[i]);
			if(index>671){
				emit energy_added(index);
				index=671;
			}
			// set an empty transaction from
			if(energy_bal.length<1){
				continue;
			}
			for(uint j=0; j<energy_bal.length; j++){
				if(energy_bal[j].energy_balancing_positive != 0){
					uint transaction_value=energy_bal[j].energy_balancing_positive*settlement_price.price_energy_balancing_positive;
					Lb.LemLib.log_transaction memory transaction_log;

					// credit supplier
					transaction_log.id_user=supplier;
					transaction_log.ts_delivery=list_ts_delivery[i];
					transaction_log.price_energy_market=settlement_price.price_energy_balancing_positive;
					transaction_log.type_transaction="balancing";
					transaction_log.qty_energy=int(-energy_bal[j].energy_balancing_positive);
					transaction_log.delta_balance=int(transaction_value);
					transaction_log.t_update_balance=ts_now;
					transaction_log.share_quality_offers_cleared_na=uint64(0);
					transaction_log.share_quality_offers_cleared_local=uint64(0);
					transaction_log.share_quality_offers_cleared_green=uint64(0);
					transaction_log.share_quality_offers_cleared_green_local=uint64(0);

					logs_transaction[index].push(transaction_log);
					clearing.update_user_balances(transaction_log);

					// debit consumer
					transaction_log.id_user=clearing.get_meter2user(energy_bal[j].id_meter);
					transaction_log.ts_delivery=list_ts_delivery[i];
					transaction_log.price_energy_market=settlement_price.price_energy_balancing_positive;
					transaction_log.type_transaction="balancing";
					transaction_log.qty_energy=int(energy_bal[j].energy_balancing_positive);
					transaction_log.delta_balance=int(-transaction_value);
					transaction_log.t_update_balance=ts_now;
					transaction_log.share_quality_offers_cleared_na=uint64(0);
					transaction_log.share_quality_offers_cleared_local=uint64(0);
					transaction_log.share_quality_offers_cleared_green=uint64(0);
					transaction_log.share_quality_offers_cleared_green_local=uint64(0);

					logs_transaction[index].push(transaction_log);
					clearing.update_user_balances(transaction_log);
				}
				else if(energy_bal[j].energy_balancing_negative!=0){
					uint transaction_value=energy_bal[j].energy_balancing_negative*settlement_price.price_energy_balancing_negative;
					Lb.LemLib.log_transaction memory transaction_log;

					// credit supplier
					transaction_log.id_user=supplier;
					transaction_log.ts_delivery=list_ts_delivery[i];
					transaction_log.price_energy_market=settlement_price.price_energy_balancing_negative;
					transaction_log.type_transaction="balancing";
					transaction_log.qty_energy=int(energy_bal[j].energy_balancing_negative);
					transaction_log.delta_balance=int(transaction_value);
					transaction_log.t_update_balance=ts_now;
					transaction_log.share_quality_offers_cleared_na=uint64(0);
					transaction_log.share_quality_offers_cleared_local=uint64(0);
					transaction_log.share_quality_offers_cleared_green=uint64(0);
					transaction_log.share_quality_offers_cleared_green_local=uint64(0);

					logs_transaction[index].push(transaction_log);
					clearing.update_user_balances(transaction_log);

					// debit consumer
					transaction_log.id_user=clearing.get_meter2user(energy_bal[j].id_meter);
					transaction_log.ts_delivery=list_ts_delivery[i];
					transaction_log.price_energy_market=settlement_price.price_energy_balancing_negative;
					transaction_log.type_transaction="balancing";
					transaction_log.qty_energy=int(-energy_bal[j].energy_balancing_negative);
					transaction_log.delta_balance=int(-transaction_value);
					transaction_log.t_update_balance=ts_now;
					transaction_log.share_quality_offers_cleared_na=uint64(0);
					transaction_log.share_quality_offers_cleared_local=uint64(0);
					transaction_log.share_quality_offers_cleared_green=uint64(0);
					transaction_log.share_quality_offers_cleared_green_local=uint64(0);

					logs_transaction[index].push(transaction_log);
					clearing.update_user_balances(transaction_log);
				}
			}
		}
	}

	function update_balance_levies(uint[] memory list_ts_delivery, uint ts_now, string memory retailer) public{
		if(list_ts_delivery.length==0){
			return;
		}
		for(uint i = 0; i < list_ts_delivery.length; i++){
			Lb.LemLib.price_settlement memory settlement_price = get_prices_settlement_by_ts(list_ts_delivery[i]);
			uint levies_pos = settlement_price.price_energy_levies_positive;
			uint levies_neg = settlement_price.price_energy_levies_negative;
			Lb.LemLib.meter_reading_delta[] memory meter_readings_delta_ts = get_meter_readings_delta_by_ts(list_ts_delivery[i]);
			uint index = lib.ts_delivery_to_index(list_ts_delivery[i]);
			if(index > 671){
				emit energy_added(index);
				index = 671;
			}
			// set an empty transaction from
			if(meter_readings_delta_ts.length < 1){
				continue;
			}

			for(uint j = 0; j < meter_readings_delta_ts.length; j++){
				if(meter_readings_delta_ts[j].energy_out != 0 && levies_pos != 0) {

					uint transaction_value = meter_readings_delta_ts[j].energy_out * levies_pos;
					Lb.LemLib.log_transaction memory transaction_log;

					// credit retailer
					transaction_log.id_user = retailer;
					transaction_log.ts_delivery = list_ts_delivery[i];
					transaction_log.price_energy_market = levies_pos;
					transaction_log.type_transaction = "levies";
					transaction_log.qty_energy = int(-meter_readings_delta_ts[j].energy_out);
					transaction_log.delta_balance = int(transaction_value);
					transaction_log.t_update_balance = ts_now;
					transaction_log.share_quality_offers_cleared_na = uint64(0);
					transaction_log.share_quality_offers_cleared_local = uint64(0);
					transaction_log.share_quality_offers_cleared_green = uint64(0);
					transaction_log.share_quality_offers_cleared_green_local = uint64(0);

					logs_transaction[index].push(transaction_log);
					clearing.update_user_balances(transaction_log);

					// debit consumer
					transaction_log.id_user = clearing.get_meter2user(meter_readings_delta_ts[j].id_meter);
					transaction_log.ts_delivery = list_ts_delivery[i];
					transaction_log.price_energy_market = levies_pos;
					transaction_log.type_transaction = "levies";
					transaction_log.qty_energy = int(meter_readings_delta_ts[j].energy_out);
					transaction_log.delta_balance = int(-transaction_value);
					transaction_log.t_update_balance = ts_now;
					transaction_log.share_quality_offers_cleared_na = uint64(0);
					transaction_log.share_quality_offers_cleared_local = uint64(0);
					transaction_log.share_quality_offers_cleared_green = uint64(0);
					transaction_log.share_quality_offers_cleared_green_local = uint64(0);

					logs_transaction[index].push(transaction_log);
					clearing.update_user_balances(transaction_log);
				}
				else if(meter_readings_delta_ts[j].energy_in != 0 && levies_neg != 0) {

					uint transaction_value=meter_readings_delta_ts[j].energy_in * levies_neg;
					Lb.LemLib.log_transaction memory transaction_log;

					// credit retailer
					transaction_log.id_user = retailer;
					transaction_log.ts_delivery = list_ts_delivery[i];
					transaction_log.price_energy_market = levies_neg;
					transaction_log.type_transaction = "levies";
					transaction_log.qty_energy = int(meter_readings_delta_ts[j].energy_in);
					transaction_log.delta_balance = int(transaction_value);
					transaction_log.t_update_balance = ts_now;
					transaction_log.share_quality_offers_cleared_na = uint64(0);
					transaction_log.share_quality_offers_cleared_local = uint64(0);
					transaction_log.share_quality_offers_cleared_green = uint64(0);
					transaction_log.share_quality_offers_cleared_green_local = uint64(0);

					logs_transaction[index].push(transaction_log);
					clearing.update_user_balances(transaction_log);

					// debit consumer
					transaction_log.id_user = clearing.get_meter2user(meter_readings_delta_ts[j].id_meter);
					transaction_log.ts_delivery = list_ts_delivery[i];
					transaction_log.price_energy_market = levies_neg;
					transaction_log.type_transaction = "levies";
					transaction_log.qty_energy = int(-meter_readings_delta_ts[j].energy_in);
					transaction_log.delta_balance = int(-transaction_value);
					transaction_log.t_update_balance = ts_now;
					transaction_log.share_quality_offers_cleared_na = uint64(0);
					transaction_log.share_quality_offers_cleared_local = uint64(0);
					transaction_log.share_quality_offers_cleared_green = uint64(0);
					transaction_log.share_quality_offers_cleared_green_local = uint64(0);

					logs_transaction[index].push(transaction_log);
					clearing.update_user_balances(transaction_log);
				}
			}
		}
	}
}
