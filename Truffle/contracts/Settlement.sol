pragma solidity >=0.5.0 <0.7.5;
pragma experimental ABIEncoderV2;

import "./ClearingExAnte.sol";
import "./Sorting.sol";

contract Settlement {

    event logString(string arg);
    Lb.LemLib lib= new Lb.LemLib();//instance of the contract LemLib(general library with useful functionalities)
	Sorting srt = new Sorting();//instance of the contract Sorting(useful sorting functionalities)
    //ClearingExAnte clearing;

	ClearingExAnte clearing;

	Lb.LemLib.meter_reading_delta[] meter_reading_deltas;
	// in solitidy, in the definition the order of indexes is inversed, so this is in reality a 672x20 matrix
	Lb.LemLib.energy_balancing[20][672] energy_balances;		//need to be constant numbers, no variables

	constructor() public{
		// since the size of the data needs to be hard coded, we check it matches the lib contract
		require(lib.get_horizon()==energy_balances.length);
		require(lib.get_num_meters()==energy_balances[0].length);
		}
	// utility implementation for not changing of contract in python
	function get_horizon()public view returns(uint){
		return lib.get_horizon();
	}
	function get_num_meters()public view returns(uint){
		return lib.get_num_meters();
	}
	// pushes the delta readings into the array
	function push_meter_readings_delta(Lb.LemLib.meter_reading_delta memory meter_delta) public {
		Settlement.meter_reading_deltas.push(meter_delta);
	}
	// function to push the energy balance to the matrix of energies
	// The rows index is the ts_delivery and the columns index is the number of meter
	function push_energy_balance(Lb.LemLib.energy_balancing memory e_balance) public {
		uint index = lib.ts_delivery_to_index(e_balance.ts_delivery);
		e_balance.meter_initialized=true;
		for(uint i=0; i<lib.get_num_meters(); i++){
			if(! energy_balances[index][i].meter_initialized){
				energy_balances[index][i]=e_balance;
				break;
			}
		}
	}

	function get_meter_readings_delta() public view returns (Lb.LemLib.meter_reading_delta[] memory){
		return Settlement.meter_reading_deltas;
	}

	//function to return the energy balance of an specific timestep
	function get_energy_balance_by_ts(uint ts) public returns(Lb.LemLib.energy_balancing[] memory){
		uint index = lib.ts_delivery_to_index(ts);
		uint count=0;
		for(uint i=0; i<lib.get_num_meters(); i++){
			if(energy_balances[index][i].meter_initialized){
				count++;
			}
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
	function get_energy_balance_all() public returns(Lb.LemLib.energy_balancing[] memory){
		uint count=0;
		for(uint i=0; i<lib.get_horizon(); i++){
			for(uint j=0; j<lib.get_num_meters();j++){
			if(energy_balances[i][j].meter_initialized){
				count++;
			}
		}
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

	// function to determine the changes in energy for a given list of time steps
	// the function calculates the change of energy for every meter inside a specific timestep
	// Finally, it pushes the results to a mapping according to the timestep
    function determine_balancing_energy(uint[] memory list_ts_delivery) public{
		//Lb.LemLib.market_result[] memory sorted_results=srt.quick_sort_market_result_ts_delivery(, true);
		for(uint i=0; i<list_ts_delivery.length; i++){
			Lb.LemLib.meter_reading_delta[] memory meters=lib.meters_delta_inside_ts_delivery(meter_reading_deltas, list_ts_delivery[i]);
			Lb.LemLib.market_result[] memory results=lib.market_results_inside_ts_delivery(clearing.getTempMarketResults(), list_ts_delivery[i]);
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
					result_energy.energy_balancing_positive=uint32(current_actual_energy);
					result_energy.energy_balancing_negative=0;
				}
				else{
					result_energy.energy_balancing_positive=0;
					result_energy.energy_balancing_negative=uint32(current_actual_energy);
				}
				Settlement.push_energy_balance(result_energy);
			}

		}
	}
}
