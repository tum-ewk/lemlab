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

	Lb.LemLib.meter_reading_delta[] meter_reading_deltas;
	// in solitidy, in the definition the order of indexes is inversed, so this is in reality a 672x20 matrix
	Lb.LemLib.energy_balancing[20][672] energy_balances;		//need to be constant numbers, no variables
	mapping(string=>uint) meter_id2index;
	bool meters_initialized=false;

	constructor(address clearing_ex_ante) public{
		// since the size of the data needs to be hard coded, we check it matches the lib contract
		require(lib.get_horizon()==energy_balances.length, "The horizon does not match the one specified in the Lib contract");
		require(lib.get_num_meters()==energy_balances[0].length, "The number of meters specified does not match the ones in the Lib contract");
		clearing=ClearingExAnte(clearing_ex_ante);
		clearing_add=clearing_ex_ante;
		}
	event energy_added(uint ts, uint meter_index);
	function clear_data() public{
		delete meter_reading_deltas;
		delete energy_balances;
	}
	function get_clearing_add() public view returns(address){
		return clearing_add;
	}
	function clear_data_gas_limit(uint max_entries, uint sec_half) public {
	    	for(uint i = 0; i < max_entries; i++){
				if(Settlement.meter_reading_deltas.length>0){
	    			Settlement.meter_reading_deltas.length--;
				}
				if(i+sec_half<lib.get_horizon()){
	    			delete Settlement.energy_balances[i+sec_half];
				}
			}
	}
	function initialize_meters() private{
		// 0 is reserved for un-initialized meters
		Lb.LemLib.id_meter[] memory id_meters = clearing.get_id_meters();
		for(uint i=0; i<id_meters.length; i++){
			meter_id2index[id_meters[i].id_meter]=i+1;
		}
		meters_initialized=true;
	}
	function get_meter2id(string memory meter_id) public view returns(uint){
		uint index=meter_id2index[meter_id]-1;
		require(index>=uint(0),"The id_meter provided was not found in the market clearing");
		return index;
	}
	// utility implementation for not changing of contract in python
	function get_horizon()public view returns(uint){
		return lib.get_horizon();
	}
	function get_num_meters()public view returns(uint){
		return lib.get_num_meters();
	}
	function get_all_meters() public view returns(uint[] memory){
		Lb.LemLib.id_meter[] memory id_meters = clearing.get_id_meters();
		uint[] memory meter_ids=new uint[](id_meters.length);
		for(uint i=0; i<id_meters.length; i++){
			meter_ids[i]=get_meter2id(id_meters[i].id_meter);
		}
		return meter_ids;
	}

	// pushes the delta readings into the array
	function push_meter_readings_delta(Lb.LemLib.meter_reading_delta memory meter_delta) public {
		Settlement.meter_reading_deltas.push(meter_delta);
	}
	// function to push the energy balance to the matrix of energies
	// The rows index is the ts_delivery and the columns index is the number of meter
	function push_energy_balance(Lb.LemLib.energy_balancing memory e_balance) public {
		uint index = lib.ts_delivery_to_index(e_balance.ts_delivery);
		uint index2=get_meter2id(e_balance.id_meter);
		e_balance.meter_initialized=true;
		emit energy_added(index, index2);
		energy_balances[index][index2]=e_balance;

	}
	function get_meter_readings_delta() public view returns (Lb.LemLib.meter_reading_delta[] memory){
		return Settlement.meter_reading_deltas;
	}


	//function to return the energy balance of an specific timestep
	function get_energy_balance_by_ts(uint ts) public view returns(Lb.LemLib.energy_balancing[] memory){
		uint index = lib.ts_delivery_to_index(ts);
		uint count=0;
		for(uint i=0; i<lib.get_num_meters(); i++){
			if(energy_balances[index][i].meter_initialized){
				count++;
			}
		}
		// if we do not find any energies for that time, we just return a one element list with the ts_delivery=-1 to easily sort that later
		if(count==0){
			Lb.LemLib.energy_balancing[] memory sample=new Lb.LemLib.energy_balancing[](1);
			sample[0].ts_delivery=uint(-1);
			return sample;
		}
		Lb.LemLib.energy_balancing[] memory results = new Lb.LemLib.energy_balancing[](count);
		count=0;
		for(uint j=0; j<lib.get_num_meters(); j++){
			if(energy_balances[index][j].meter_initialized){
				results[count]=energy_balances[index][j];
				count++;
			}
		}
		return results;
	}
	function get_energy_balance_all() public view returns(Lb.LemLib.energy_balancing[] memory){
		uint count=0;
		for(uint i=0; i<lib.get_horizon(); i++){
			for(uint j=0; j<lib.get_num_meters();j++){
				if(energy_balances[i][j].meter_initialized){
					count++;
				}
			}
		}
		if(count==0){
			Lb.LemLib.energy_balancing[] memory sample=new Lb.LemLib.energy_balancing[](1);
			sample[0].ts_delivery=uint(-1);
			return sample;
		}
		Lb.LemLib.energy_balancing[] memory results = new Lb.LemLib.energy_balancing[](count);
		count=0;
		for(uint i=0; i<lib.get_horizon(); i++){
			for(uint j=0; j<lib.get_num_meters();j++){
			if(energy_balances[i][j].meter_initialized){
				results[count]=energy_balances[i][j];
				count++;
			}
		}
		}
		return results;
	}
	// we get the total market results, as the temp list is empty
	function get_market_results_total() public view returns(Lb.LemLib.market_result_total[] memory){
		return clearing.getMarketResultsTotal();
	}

	// function to determine the changes in energy for a given list of time steps
	// the function calculates the change of energy for every meter inside a specific timestep
	// Finally, it pushes the results to a mapping according to the timestep
    function determine_balancing_energy(uint[] memory list_ts_delivery) public{
		//Lb.LemLib.market_result[] memory sorted_results=srt.quick_sort_market_result_ts_delivery(, true);
		if(list_ts_delivery.length==0){
			return;
		}
		if(!meters_initialized){
			initialize_meters();
		}
		for(uint i=0; i<list_ts_delivery.length; i++){
			Lb.LemLib.meter_reading_delta[] memory meters=lib.meters_delta_inside_ts_delivery(get_meter_readings_delta(), list_ts_delivery[i]);
			Lb.LemLib.market_result_total[] memory results=lib.market_results_inside_ts_delivery(get_market_results_total(), list_ts_delivery[i]);

			if(meters[0].ts_delivery<uint(0) || results[0].ts_delivery<uint(0)){
				continue;	// this means that no meters or market_results were found for that ts
			}
			for(uint j=0; j<meters.length;j++){
				uint current_actual_energy=meters[j].energy_out-meters[j].energy_in;
				uint current_market_energy=0;
				for(uint k=0; k<results.length;k++){
					if(lib.compareStrings(meters[j].id_meter, results[k].id_user_bid)){
						current_market_energy -=  results[k].qty_energy_traded;
					}
					else if(lib.compareStrings(meters[j].id_meter, results[k].id_user_offer)){
						current_market_energy += results[k].qty_energy_traded;
					}
				}
				current_actual_energy -= current_market_energy;
				Lb.LemLib.energy_balancing memory result_energy;
				result_energy.id_meter=meters[j].id_meter;
				result_energy.ts_delivery=list_ts_delivery[i];
				result_energy.meter_initialized=false;
				// in a similar way to python´s decompose float function, we store the difference in energy if positive or negative
				if(current_actual_energy>=0){
					result_energy.energy_balancing_positive=current_actual_energy;
					result_energy.energy_balancing_negative=0;
				}
				else{
					result_energy.energy_balancing_positive=0;
					result_energy.energy_balancing_negative=-current_actual_energy;
				}
				Settlement.push_energy_balance(result_energy);
			}

		}
	}
}
