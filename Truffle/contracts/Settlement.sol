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

    function determine_balancing_energy(uint[] memory list_ts_delivery) public{
		Lb.LemLib.market_result[] memory sorted_results=srt.quick_sort_market_result_ts_delivery(clearing.getTempMarketResults(), true);
		for(uint i=0; i<list_ts_delivery.length; i++){
			Lb.LemLib.meter_reading_delta[] memory meters=lib.meters_delta_inside_ts_delivery(clearing.get_meter_readings_delta(), list_ts_delivery[i]);
			Lb.LemLib.market_result[] memory results=lib.market_results_inside_ts_delivery(sorted_results, list_ts_delivery[i]);
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
				// in a similar way to python´s decompose float function, we store the difference in energy if positive or negative
				if(current_actual_energy>=0){
					result_energy.energy_balancing_positive=current_actual_energy;
					result_energy.energy_balancing_negative=0;
				}
				else{
					result_energy.energy_balancing_positive=0;
					result_energy.energy_balancing_negative=current_actual_energy;
				}
				clearing.push_energy_balance(result_energy, list_ts_delivery[i]);
			}

		}
	}
}
