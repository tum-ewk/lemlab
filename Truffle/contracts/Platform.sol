pragma solidity >=0.5.0 <0.7.5;
pragma experimental ABIEncoderV2;

import "./Param.sol";
import "./Sorting.sol";
//import "@nomiclabs/buidler/console.sol";

contract Platform {

	/* this is an event which receives one string as argument.
	every event produces a log at the end of a transaction. this event can be used for debugging(not live).
	*/
	event logString(string arg);
	Lb.Lib.offer_bid[] offers;//list of total offers stored. they don't get deleted unless one reset the contract
	Lb.Lib.offer_bid[] bids;//list of total bids stored. they don't get deleted unless one reset the contract
	Lb.Lib.user_info[] user_infos;
	Lb.Lib.id_meter[] id_meters;
	/*list of temporary offers stored. 
	They are relative to each market clearing and for every market clearing they might be deleted. 
	Now the deletion is performed via Web3.py in python before pushing new ones.*/
	Lb.Lib.offer_bid[] temp_offers;
	/*list of temporary bids stored. 
	They are relative to each market clearing and for every market clearing they might be deleted. 
	Now the deletion is performed via Web3.py in python before pushing new ones.*/
	Lb.Lib.offer_bid[] temp_bids;
	Lb.Lib.market_result[] temp_market_results;//market results for each single clearing(for each specific t_clearing_current)
	Lb.Lib.market_result_total[] market_results_total;//whole market results relative to the whole market clearing(96 loops)
	string public string_to_log = "";//string used for the event logString
	Param p = new Param();//instance of the contract Param
	Lb.Lib lib= new Lb.Lib();//instance of the contract Lib(general library with useful functionalities)
	Sorting srt = new Sorting();//instance of the contract Sorting(useful sorting functionalities)

	//mapping(uint => Lb.Lib.energy_balancing) public energy_balances;
	Lb.Lib.meter_reading_delta[] meter_reading_deltas;
	Lb.Lib.energy_balancing[] energy_balances;

	constructor() public{
		Platform.clearTempData();//constructor where all the data is cleared.
		}

	//function to delete a single user from the chain, for that, we push the last one into
	// that position and delete the last (decreasing the length auto deletes the last element at a cheaper gas cost), to not leave any empty spot
	// the function can additionally delete all the meters pointing to that user
	function delete_user(Lb.Lib.user_info memory user, bool del_meters) public {
		for(uint i=0; i<Platform.user_infos.length; i++){
			if(keccak256(abi.encode(Platform.user_infos[i].id_user))==keccak256(abi.encode(user.id_user))){
				Platform.user_infos[i]=Platform.user_infos[Platform.user_infos.length-1];
				Platform.user_infos.length--;
				break;
			}
		}
		if (del_meters){
			for(uint j=0; j<Platform.id_meters.length; j++){
				if(keccak256(abi.encode(Platform.id_meters[j].id_user))==keccak256(abi.encode(user.id_user))){
					Platform.id_meters[j]=Platform.id_meters[Platform.id_meters.length-1];
					Platform.id_meters.length--;
				}
			}
		}
	}
	//function to delete a single meter from the chain
	function delete_meter(Lb.Lib.id_meter memory meter) public{
		for(uint i=0; i<Platform.id_meters.length; i++){
			if(keccak256(abi.encode(Platform.id_meters[i].id_meter))==keccak256(abi.encode(meter.id_meter))){
				Platform.id_meters[i]=Platform.id_meters[Platform.id_meters.length-1];
				Platform.id_meters.length--;
				break;
			}
		}
	}

	function clearTempData() public {//function that deletes objects from the contract storage
	    delete Platform.temp_offers;
		delete Platform.temp_bids;
		delete Platform.temp_market_results;
		delete Platform.market_results_total;
	}
	function clearPermanentData() public {//function that deletes objects from the contract storage
		delete Platform.user_infos;
		delete Platform.id_meters;
		delete Platform.offers;
		delete Platform.bids;
	}
	/*
	same function as clearTempData(). It is used when clearTempData() exceeds the gas limit.
	In this case, variables have to be deleted by chunks. Reducing automatically the length deletes the last element and costs less gas
	*/
	function clearTempData_gas_limit(uint max_entries) public {
	    for(uint i = 0; i < max_entries; i++){
	    	if(Platform.temp_offers.length > 0){
	    		//delete Platform.temp_offers[Platform.temp_offers.length - 1];
	    		Platform.temp_offers.length--;
	    	}
	    	if(Platform.temp_bids.length > 0){
	    		//delete Platform.temp_bids[Platform.temp_bids.length - 1];
	    		Platform.temp_bids.length--;
	    	}
	    	if(Platform.temp_market_results.length > 0){
	    		//delete Platform.temp_market_results[Platform.temp_market_results.length - 1];
	    		Platform.temp_market_results.length--;
	    	}
	    }
	}
	/*
	similar function to clearTempData_gas_limit(). It is used when clearPermanentData() exceeds the gas limit.
	In this case, variables have to be deleted by chunks
	*/
	function clearPermanentData_gas_limit(uint max_entries) public {
	    for(uint i = 0; i < max_entries; i++){
	    	if(Platform.offers.length > 0){
	    		//delete Platform.offers[Platform.offers.length - 1];
	    		Platform.offers.length--;
	    	}
	    	if(Platform.bids.length > 0){
	    		//delete Platform.bids[Platform.bids.length - 1];
	    		Platform.bids.length--;
	    	}
	    	if(Platform.market_results_total.length > 0){
	    		//delete Platform.market_results_total[Platform.market_results_total.length - 1];
	    		Platform.market_results_total.length--;
	    	}
	    	if(Platform.user_infos.length > 0){
	    		//delete Platform.user_infos[Platform.user_infos.length - 1];
	    		Platform.user_infos.length--;
	    	}
	    	if(Platform.id_meters.length > 0){
	    		//delete Platform.id_meters[Platform.id_meters.length - 1];
	    		Platform.id_meters.length--;
	    	}
	    }
	}
	//add an offer or bid to the list of temporary and/or permanent offers in the storage of the contract
	function pushOfferOrBid(Lb.Lib.offer_bid memory ob, bool isOffer, bool temp, bool permanent) public {
	    if(isOffer) pushOffer(ob, temp, permanent);
		else pushBid(ob, temp, permanent);
	}
	//add an offer to the lists of temporary and/or permanent offers in the storage of the contract
	function pushOffer(Lb.Lib.offer_bid memory off, bool temp, bool permanent) private {
		if(temp) Platform.temp_offers.push(off);
		if(permanent) Platform.offers.push(off);
	}
	//add an bid to the lists of temporary and/or permanent bids in the storage of the contract
	function pushBid(Lb.Lib.offer_bid memory bid, bool temp, bool permanent) private {
		if(temp) Platform.temp_bids.push(bid);
		if(permanent) Platform.bids.push(bid);
	}
	//add a user info to the lists of user_infos in the storage of the contract
	function push_user_info(Lb.Lib.user_info memory user_info) public {
	    /*bool go_on = true;
	    uint i = 0;
	    while(go_on && i < Platform.user_infos.length) {
	        if(lib.compareStrings(Platform.user_infos[i].id_user, user_info.id_user)) {
                Platform.user_infos[i] = user_info;
                go_on = false;
            }
	        i++;
	    }
	    if(go_on) {
	        Platform.user_infos.push(user_info);
	    }*/
	    Platform.user_infos.push(user_info);
	}
	//add a id_meter to the lists of id_meters in the storage of the contract
	function push_id_meters(Lb.Lib.id_meter memory id_meter) public {
		Platform.id_meters.push(id_meter);
	}
	//gets the list of user_infos in the storage of the contract
	function get_user_infos() public view returns (Lb.Lib.user_info[] memory) {
		return Platform.user_infos;
	}
	//gets the list of id_meters in the storage of the contract
	function get_id_meters() public view returns (Lb.Lib.id_meter[] memory) {
		return Platform.id_meters;
	}
	//gets the list of temporary or permanent offers
	function getOffers(bool temp) public view returns(Lb.Lib.offer_bid[] memory) {
	    if(temp) return Platform.temp_offers;
		else return Platform.offers;
	}
	//gets the list of temporary or permanent bids
    function getBids(bool temp) public view returns(Lb.Lib.offer_bid[] memory) {
		if(temp) return Platform.temp_bids;
		else return Platform.bids;
    }
    //gets the total market results
	function getMarketResultsTotal() public view returns (Lb.Lib.market_result_total[] memory) {
	    return Platform.market_results_total;
	}
	//gets the temporary market results
	function getTempMarketResults() public view returns (Lb.Lib.market_result[] memory) {
	    return Platform.temp_market_results;
	}
	//returns a filtering of the temporary positions(offer/bids). The ones having the ts_delivery == t_clearing_current and t_clearing_current in the limits of the ts_delivery first and last of the user
	function filteredOffersBids_ts_delivery_user(uint t_clearing_current) public view returns (Lb.Lib.offer_bid[] memory, Lb.Lib.offer_bid[] memory){
		uint len = 0;
		uint j = 0;
		for (uint i=0; i<Platform.temp_offers.length; i++) {
			if ( Platform.temp_offers[i].ts_delivery == t_clearing_current && lib.check_user_id_in_user_infos_interval(Platform.temp_offers[i].id_user, t_clearing_current, Platform.user_infos)) {
		    	len++;
			}
		}

		Lb.Lib.offer_bid[] memory filtered_offers = new Lb.Lib.offer_bid[](len);

		for (uint i=0; i<Platform.temp_offers.length; i++) {
			if ( Platform.temp_offers[i].ts_delivery == t_clearing_current && lib.check_user_id_in_user_infos_interval(Platform.temp_offers[i].id_user, t_clearing_current, Platform.user_infos)) {
		    	filtered_offers[j] = Platform.temp_offers[i];
		    	j++;
			}
	    }

	    len = 0;
	    j=0;

		for (uint i=0; i<Platform.temp_bids.length; i++) {
			if ( Platform.temp_bids[i].ts_delivery == t_clearing_current && lib.check_user_id_in_user_infos_interval(Platform.temp_bids[i].id_user, t_clearing_current, Platform.user_infos)) {
		    	len++;
			}
		}

		Lb.Lib.offer_bid[] memory filtered_bids = new Lb.Lib.offer_bid[](len);

		for (uint i=0; i<Platform.temp_bids.length; i++) {
			if ( Platform.temp_bids[i].ts_delivery == t_clearing_current && lib.check_user_id_in_user_infos_interval(Platform.temp_bids[i].id_user, t_clearing_current, Platform.user_infos)) {
		    	filtered_bids[j] = Platform.temp_bids[i];
		    	j++;
			}
		}
	    return (filtered_offers, filtered_bids);
	}
	//returns a filtering of the temporary positions(offer/bids). The ones having the ts_delivery == t_clearing_current and quantity of energy > 0
	function getFilteredOffersBids_memory(uint t_clearing_current) public view returns (Lb.Lib.offer_bid[] memory, Lb.Lib.offer_bid[] memory){
	    uint len = 0;
		uint j = 0;
		for (uint i=0; i<Platform.temp_offers.length; i++) {
			if ( Platform.temp_offers[i].ts_delivery == t_clearing_current && Platform.temp_offers[i].qty_energy > 0) {
		    	len++;
			}
		}

		Lb.Lib.offer_bid[] memory filtered_offers = new Lb.Lib.offer_bid[](len);

		for (uint i=0; i<Platform.temp_offers.length; i++) {
			if ( Platform.temp_offers[i].ts_delivery == t_clearing_current && Platform.temp_offers[i].qty_energy > 0) {
		    	filtered_offers[j] = Platform.temp_offers[i];
		    	j++;
			}
	    }

	    len = 0;
	    j=0;

		for (uint i=0; i<Platform.temp_bids.length; i++) {
			if ( Platform.temp_bids[i].ts_delivery == t_clearing_current && Platform.temp_bids[i].qty_energy > 0) {
		    	len++;
			}
		}

		Lb.Lib.offer_bid[] memory filtered_bids = new Lb.Lib.offer_bid[](len);

		for (uint i=0; i<Platform.temp_bids.length; i++) {
			if ( Platform.temp_bids[i].ts_delivery == t_clearing_current && Platform.temp_bids[i].qty_energy > 0) {
		    	filtered_bids[j] = Platform.temp_bids[i];
		    	j++;
			}
		}
	    return (filtered_offers, filtered_bids);
	}
	//same as getFilteredOffersBids_memory(). takes more gas than that though
	function getFilteredOffersBids_memory_two(uint t_clearing_current) public view returns (Lb.Lib.offer_bid[] memory, Lb.Lib.offer_bid[] memory){
		Lb.Lib.offer_bid[] memory filtered_offers = new Lb.Lib.offer_bid[](Platform.temp_offers.length);
		uint j = 0;
		for (uint i=0; i<Platform.temp_offers.length; i++) {
			if ( Platform.temp_offers[i].ts_delivery == t_clearing_current ) {
		    	filtered_offers[j] = Platform.temp_offers[i];
		    	j++;
			}
	    }
	    filtered_offers = lib.cropOfferBids(filtered_offers, 0, j-1);
		Lb.Lib.offer_bid[] memory filtered_bids = new Lb.Lib.offer_bid[](Platform.temp_bids.length);
		j = 0;

		for (uint i=0; i<Platform.temp_bids.length; i++) {
			if ( Platform.temp_bids[i].ts_delivery == t_clearing_current ) {
		    	filtered_bids[j] = Platform.temp_bids[i];
		    	j++;
			}
		}
		filtered_bids = lib.cropOfferBids(filtered_bids, 0, j-1);
	    return (filtered_offers, filtered_bids);
	}
	//same as getFilteredOffersBids_memory(). it also sorts the position by price
	function filter_sort_OffersBids_memory(uint t_clearing_current) public view returns(Lb.Lib.offer_bid[] memory, Lb.Lib.offer_bid[] memory) {
	    Lb.Lib.offer_bid[] memory filtered_offers;
	    Lb.Lib.offer_bid[] memory filtered_bids;
	    (filtered_offers, filtered_bids) = getFilteredOffersBids_memory(t_clearing_current);
	    filtered_offers = srt.quickSortOffersBidsPrice(filtered_offers, true);
	    filtered_bids = srt.quickSortOffersBidsPrice(filtered_bids, false);
	    return (filtered_offers, filtered_bids);
	}
	//similar to filter_sort_OffersBids_memory(). It also sorts by quality(and optional quantity) and aggregate identical positions
	function filter_sort_aggregate_OffersBids_memory(uint t_clearing_current, bool simulation_test) public view returns(Lb.Lib.offer_bid[] memory, Lb.Lib.offer_bid[] memory) {
	    Lb.Lib.offer_bid[] memory filtered_offers;
	    Lb.Lib.offer_bid[] memory filtered_bids;
	    (filtered_offers, filtered_bids) = getFilteredOffersBids_memory(t_clearing_current);

	    filtered_offers = srt.aggregate_identical_positions(filtered_offers, simulation_test);
	    filtered_bids = srt.aggregate_identical_positions(filtered_bids, simulation_test);

	    filtered_offers = srt.insertionSortOffersBidsPrice_Quality(filtered_offers, true, false, simulation_test, false);
	    filtered_bids = srt.insertionSortOffersBidsPrice_Quality(filtered_bids, false, false, simulation_test, false);
	    return (filtered_offers, filtered_bids);
	}
	//add one supplier bid and one supplier offer to the lists of offers and bids given in input. then it returns them.
	function add_supplier_bids_memory(uint t_clearing_current, Lb.Lib.offer_bid[] memory filtered_offers, Lb.Lib.offer_bid[] memory filtered_bids) public view returns(Lb.Lib.offer_bid[] memory, Lb.Lib.offer_bid[] memory) {
	    Lb.Lib.offer_bid memory supOfferBid = Lb.Lib.offer_bid({id_user:p.getIdSupplier(), qty_energy: p.getQtyOfferSupplier(), price_energy: p.getPriceOfferSupplier(), quality_energy:0, type_position:"0", premium_preference_quality:p.getPremium_preference_quality(), number_position:0, status_position:0, t_submission:t_clearing_current, ts_delivery:t_clearing_current});
		
		Lb.Lib.offer_bid[] memory filtered_offers_sup = new Lb.Lib.offer_bid[](filtered_offers.length+1);
		filtered_offers_sup[0] = supOfferBid;
		for(uint i=0; i<filtered_offers.length; i++) {
		    filtered_offers_sup[i+1] = filtered_offers[i];
		}
		
	    supOfferBid = Lb.Lib.offer_bid({ts_delivery:t_clearing_current, price_energy: p.getPriceBidSupplier(), number_position:0, t_submission:t_clearing_current, id_user:p.getIdSupplier(), qty_energy: p.getQtyBidSupplier(), status_position:0, type_position:"1", quality_energy:0, premium_preference_quality:p.getPremium_preference_quality()});
	    
	    Lb.Lib.offer_bid[] memory filtered_bids_sup = new Lb.Lib.offer_bid[](filtered_bids.length+1);
		filtered_bids_sup[0] = supOfferBid;
		for(uint i=0; i<filtered_bids.length; i++) {
		    filtered_bids_sup[i+1] = filtered_bids[i];
		}
	    return (filtered_offers_sup, filtered_bids_sup);
	}
	//single clearing, relative for a specific t_clearing_current. At the end it can push the results temp_market_results and temp_market_result_total
	function single_clearing(uint t_clearing_current, bool add_supplier_bids, bool uniform_pricing, bool discriminative_pricing, uint t_cleared, bool writeTempMarketResult, bool writeFinalMarketResult, bool verbose, bool shuffle, bool simulation_test) public {
		if(writeTempMarketResult) delete Platform.temp_market_results;

		Lb.Lib.offer_bid[] memory filtered_offers;
	    Lb.Lib.offer_bid[] memory filtered_bids;
	    //filtering offers and bids by t_clearing_current
	    (filtered_offers, filtered_bids) = getFilteredOffersBids_memory(t_clearing_current);
	    //aggregating offers and bids by with same price, user_id, quality
	    filtered_offers = srt.aggregate_identical_positions(filtered_offers, simulation_test);
	   	filtered_bids = srt.aggregate_identical_positions(filtered_bids, simulation_test);

	    //Check whether the flag supplier bids is true
    	//Insert supplier bids and offers
        if (add_supplier_bids) {
        	(filtered_offers, filtered_bids) = Platform.add_supplier_bids_memory(t_clearing_current, filtered_offers, filtered_bids);
        }

        //Check whether offers or bids are empty
        if( filtered_offers.length == 0 || filtered_bids.length == 0 ) {
        	if(verbose) string_to_log = lib.concatenateStrings(string_to_log,"\tNo clearing - supply and/or bids are empty\n");
        }
        
        else { //Offers and bids are not empty
			
			if(verbose) string_to_log = lib.concatenateStrings(string_to_log,"\tLength of offers and bids > 0. Starting clearing\n");
            if(shuffle) {//shuffling positions
		    	filtered_offers = lib.shuffle_OfferBids(filtered_offers);
		    	filtered_bids = lib.shuffle_OfferBids(filtered_bids);
	    	}
	    	//sorting positions by price, quality, and possibly quantity
            filtered_offers = srt.insertionSortOffersBidsPrice_Quality(filtered_offers, true, false, simulation_test, false);
	    	filtered_bids = srt.insertionSortOffersBidsPrice_Quality(filtered_bids, false, false, simulation_test, false);

	    	if(verbose) {
	    		string_to_log = lib.concatenateStrings(lib.concatenateStrings(string_to_log,lib.concatenateStrings("\tOffers length: ",lib.uintToString(filtered_offers.length))),"\n");
				string_to_log = lib.concatenateStrings(lib.concatenateStrings(string_to_log,lib.concatenateStrings("\tBids length: ",lib.uintToString(filtered_bids.length))),"\n");
	    	}

            Lb.Lib.market_result[] memory tmp_market_results;

            //merging filtered offers and bids
            tmp_market_results = merge_offers_bids_memory(filtered_offers, filtered_bids);
            if(verbose) string_to_log = lib.concatenateStrings(lib.concatenateStrings(string_to_log,lib.concatenateStrings("\tMerge offers/bid length: ",lib.uintToString(tmp_market_results.length))),"\n");
            
            //calculating uniform and discriminative pricing
            tmp_market_results = calc_market_clearing_prices(tmp_market_results, uniform_pricing, discriminative_pricing);

            if(writeTempMarketResult) {
            	for(uint i = 0; i < tmp_market_results.length; i++) {
                	Platform.temp_market_results.push(tmp_market_results[i]);
            	}
            }
            if(verbose) string_to_log = lib.concatenateStrings(string_to_log,"\tCalculated clearing prices\n");
            
            //Check whether market has cleared a volume
            if(writeFinalMarketResult && tmp_market_results.length > 0) {
            	//time costly approach!
            	//Challenge storage c = challenges[challenges.length - 1];
            	Lb.Lib.market_result_total memory temp_market_result_total;
            	for(uint i = 0; i<tmp_market_results.length; i++) {
            	    temp_market_result_total = Lb.Lib.market_result_total(
            	        {
                        id_user_offer:tmp_market_results[i].id_user_offer,
                        price_energy_offer:tmp_market_results[i].price_energy_offer,
                        number_position_offer:tmp_market_results[i].number_position_offer,
                        ts_delivery:tmp_market_results[i].ts_delivery,
                        id_user_bid:tmp_market_results[i].id_user_bid,
                        price_energy_bid:tmp_market_results[i].price_energy_bid,
                        number_position_bid:tmp_market_results[i].number_position_bid,
                        price_energy_market_uniform:tmp_market_results[i].price_energy_market_uniform,
                        price_energy_market_discriminative:tmp_market_results[i].price_energy_market_discriminative,
                        qty_energy_traded:tmp_market_results[i].qty_energy_traded,
                        share_quality_NA:tmp_market_results[i].share_quality_NA,
        				share_quality_local:tmp_market_results[i].share_quality_local,
        				share_quality_green:tmp_market_results[i].share_quality_green,
        				share_quality_green_local:tmp_market_results[i].share_quality_green_local,
                        t_cleared:t_cleared
            	    });
            	    Platform.market_results_total.push(temp_market_result_total);
            	}
            }
            else if (verbose) {
                string_to_log=lib.concatenateStrings(string_to_log,"\tMarket Volume == 0 or empty market results for this clearing\n");
                string_to_log=lib.concatenateStrings(lib.concatenateStrings(string_to_log,lib.concatenateStrings("\tMarket Results length:",lib.uintToString(Platform.temp_market_results.length))),"\n");
            }
        }
	}
	//same as single_clearing(). It performs the operations in memory and return the temp market results(i.e. for this single clearing)
	function single_clearing_memory(uint t_clearing_current, bool add_supplier_bids, bool uniform_pricing, bool discriminative_pricing, bool shuffle, bool simulation_test) public view returns(Lb.Lib.market_result[] memory){
		Lb.Lib.offer_bid[] memory filtered_offers;
	    Lb.Lib.offer_bid[] memory filtered_bids;
	    (filtered_offers, filtered_bids) = getFilteredOffersBids_memory(t_clearing_current);
	    //(filtered_offers, filtered_bids) = filteredOffersBids_ts_delivery_user(t_clearing_current);

	    filtered_offers = srt.aggregate_identical_positions(filtered_offers, simulation_test);
	   	filtered_bids = srt.aggregate_identical_positions(filtered_bids, simulation_test);

	    //Check whether this is the first clearing period and whether the flag supplier bids is true
    	//Insert supplier bids and offers
        if (add_supplier_bids) {
        	(filtered_offers, filtered_bids) = Platform.add_supplier_bids_memory(t_clearing_current, filtered_offers, filtered_bids);
        }
        if(filtered_offers.length > 0 && filtered_bids.length > 0) { //Offers and bids are not empty and last update in offers/bids is newer than last clearing time
            if(shuffle) {
		    	filtered_offers = lib.shuffle_OfferBids(filtered_offers);
		    	filtered_bids = lib.shuffle_OfferBids(filtered_bids);
	    	}

            filtered_offers = srt.insertionSortOffersBidsPrice_Quality(filtered_offers, true, false, simulation_test, false);
	    	filtered_bids = srt.insertionSortOffersBidsPrice_Quality(filtered_bids, false, false, simulation_test, false);

            Lb.Lib.market_result[] memory tmp_market_results = merge_offers_bids_memory(filtered_offers, filtered_bids);
            
            tmp_market_results = calc_market_clearing_prices(tmp_market_results, uniform_pricing, discriminative_pricing);

            return temp_market_results;
        }
        return new Lb.Lib.market_result[](0);
	}
	//it performs the merge between a list of offers, and a list of bids. it produces an object of the type market_result.
	function merge_offers_bids_memory(Lb.Lib.offer_bid[] memory filtered_offers, Lb.Lib.offer_bid[] memory filtered_bids) public view returns(Lb.Lib.market_result[] memory) {
	    //Insert cumulated bid energy into tables
	    uint[] memory energy_cumulated_offers = lib.getEnergyCumulated(filtered_offers);
	    uint[] memory energy_cumulated_bids = lib.getEnergyCumulated(filtered_bids);
	    
	    //merge bids and offers
	    
	    uint i = 0;
	    uint j = 0;
	    uint z = 0;
	    uint energy_cumulated;
	    Lb.Lib.market_result memory merge;
	    //max length of results == filtered_offers.length + filtered_bids.length
	    Lb.Lib.market_result[] memory temp_market_results_m = new Lb.Lib.market_result[](filtered_offers.length + filtered_bids.length);
	    uint[] memory energy_cumulated_finals = new uint[](filtered_offers.length + filtered_bids.length);

	    while(i < energy_cumulated_offers.length && j < energy_cumulated_bids.length) {
	            if (energy_cumulated_offers[i] <= energy_cumulated_bids[j]) {
	                energy_cumulated = energy_cumulated_offers[i];
	            }
	            else {
	                energy_cumulated = energy_cumulated_bids[j];
	            }
	        merge = Lb.Lib.market_result(
	                {
	                    id_user_offer:filtered_offers[i].id_user,
	                    qty_energy_offer:0,
	                    price_energy_offer:filtered_offers[i].price_energy,
	                    quality_energy_offer:filtered_offers[i].quality_energy,
	                    premium_preference_quality_offer:filtered_offers[i].premium_preference_quality,
	                    type_position_offer:filtered_offers[i].type_position,
	                    number_position_offer:filtered_offers[i].number_position,
	                    status_position_offer:filtered_offers[i].status_position,
	                    t_submission_offer:filtered_offers[i].t_submission,
	                    ts_delivery:filtered_offers[i].ts_delivery,
	                    id_user_bid:filtered_bids[j].id_user,
	                    qty_energy_bid:0,
	                    price_energy_bid:filtered_bids[j].price_energy,
	                    quality_energy_bid:filtered_bids[j].quality_energy,
	                    premium_preference_quality_bid:filtered_bids[j].premium_preference_quality,
	                    type_position_bid:filtered_bids[j].type_position,
	                    number_position_bid:filtered_bids[j].number_position,
	                    status_position_bid:filtered_bids[j].status_position,
	                    t_submission_bid:filtered_bids[j].t_submission,
	                    price_energy_market_uniform:0,
	                    price_energy_market_discriminative:0,
	                    qty_energy_traded:0,
	                    share_quality_NA:0,
	                    share_quality_local:0,
	                    share_quality_green:0,
	                    share_quality_green_local:0
	                });
	       
	        if (energy_cumulated_offers[i] == energy_cumulated_bids[j]) {
	            i += 1;
	            j += 1;
	        }
	        else {
	                if (energy_cumulated_offers[i] < energy_cumulated_bids[j]) {
	                    i += 1;
	                }
	                else {
	                    j += 1;
	                }
	        }
	        //extract all merged bids and offers where offer price is lower or equal than the bid price
	        if( merge.price_energy_offer <= merge.price_energy_bid ) {
	            temp_market_results_m[z] = merge;
	            energy_cumulated_finals[z] = energy_cumulated;
	            z++;
	        }
	    }
	    
	    //z basically equals the length

	   	if(z <= 0) {
	    	return new Lb.Lib.market_result[](0);
	    }
	    //calculating the differences in the array of energy cumulated
	    uint[] memory qties_energy_traded = new uint[](z);
	    uint[] memory qtys_difference = lib.computeDifferences(energy_cumulated_finals, 0, z - 1);
	    qties_energy_traded[0] = energy_cumulated_finals[0];
	    //assigning those differences to the array of energy traded
	    for(i = 1; i < qties_energy_traded.length; i++) {
	    	qties_energy_traded[i] = qtys_difference[i-1];
	    }
	    
	    //modify temp_market_results_m length
        Lb.Lib.market_result[] memory temp_market_results_final = new Lb.Lib.market_result[](z);
        for(i = 0; i < z; i++) {
            temp_market_results_final[i] = temp_market_results_m[i];
            temp_market_results_final[i].qty_energy_offer = qties_energy_traded[i];
            temp_market_results_final[i].qty_energy_bid = qties_energy_traded[i];
            temp_market_results_final[i].qty_energy_traded = qties_energy_traded[i];
        }
        temp_market_results_final = addTypesQuality_share(temp_market_results_final);

	    return temp_market_results_final;
	}
	//it calculates the share of source of energy, for every single match offer/bid
	function addTypesQuality_share(Lb.Lib.market_result[] memory temp_market_results_m) public pure returns(Lb.Lib.market_result[] memory) {
		//{0: 'NA', 1: 'local', 2: 'green', 3: 'green_local'}
        uint qty = 0;
        uint sum_qty_energy_traded = 0;
        uint share_type_na = 0;
        uint share_type_local = 0;
        uint share_type_green = 0;
        uint share_type_green_local = 0;
        for(uint i = 0; i < temp_market_results_m.length; i++) {
        	qty = temp_market_results_m[i].qty_energy_traded;
        	sum_qty_energy_traded = sum_qty_energy_traded + qty;
    		if(temp_market_results_m[i].quality_energy_offer == 0) share_type_na = share_type_na + qty;
    		else if(temp_market_results_m[i].quality_energy_offer == 1) share_type_local = share_type_local + qty;
    		else if(temp_market_results_m[i].quality_energy_offer == 2) share_type_green = share_type_green + qty;
    		else if(temp_market_results_m[i].quality_energy_offer == 3) share_type_green_local = share_type_green_local + qty;
    	}
    	for(uint i = 0; i < temp_market_results_m.length; i++) {
    		temp_market_results_m[i].share_quality_NA = (share_type_na * 100) / sum_qty_energy_traded;
    		temp_market_results_m[i].share_quality_local = (share_type_local * 100) / sum_qty_energy_traded;
    		temp_market_results_m[i].share_quality_green = (share_type_green * 100) / sum_qty_energy_traded;
    		temp_market_results_m[i].share_quality_green_local = (share_type_green_local * 100) / sum_qty_energy_traded;
    	}
    	return temp_market_results_m;
	}
	//calculates the uniform and discriminative pricing of market_result given in input
	function calc_market_clearing_prices(Lb.Lib.market_result[] memory temp_market_results_m, bool uniform_pricing, bool discriminative_pricing) public pure returns(Lb.Lib.market_result[] memory) {
	    //check whether merged bids and offers are empty
	    if (temp_market_results_m.length > 0) {
	        if(uniform_pricing) {
	            //Calculate market clearing price by taking average of last matching bids
	            //In Solidity, division rounds towards zero
	            uint price_cleared_uniform = (temp_market_results_m[temp_market_results_m.length - 1].price_energy_offer + temp_market_results_m[temp_market_results_m.length - 1].price_energy_bid)/2;
	            for(uint i = 0; i < temp_market_results_m.length; i++) {
	                temp_market_results_m[i].price_energy_market_uniform = price_cleared_uniform;
	            }
	        }
	        if(discriminative_pricing) {
	            //In Solidity, division rounds towards zero
	            for(uint i = 0; i < temp_market_results_m.length; i++) {
	                temp_market_results_m[i].price_energy_market_discriminative = (temp_market_results_m[i].price_energy_offer + temp_market_results_m[i].price_energy_bid)/2;
	            }
	        }
	    }
	    return temp_market_results_m;
	}
	//update the balances of the user infos in the storage, given the total market results on storage as well. It returns the user infos with the updated balances
	function updateBalances_call() public view returns(Lb.Lib.user_info[] memory) {
		Lb.Lib.user_info[] memory temp_balance_update = lib.copyArray_UserInfo(Platform.user_infos, 0, Platform.user_infos.length - 1);
		for(uint i = 0; i < Platform.market_results_total.length; i++) {
			int delta = int(Platform.market_results_total[i].price_energy_market_uniform * Platform.market_results_total[i].qty_energy_traded);//I don't divide by 1000, since there is no float
			for(uint j = 0; j < temp_balance_update.length; j++) {
				if(delta < 0) delta = (-1) * delta;
				string memory id_user = temp_balance_update[j].id_user;
				if(lib.compareStrings(Platform.market_results_total[i].id_user_offer, id_user) || lib.compareStrings(Platform.market_results_total[i].id_user_bid, id_user)) {
					if(!(lib.compareStrings(Platform.market_results_total[i].id_user_offer,Platform.market_results_total[i].id_user_bid))) {
						if(lib.compareStrings(Platform.market_results_total[i].id_user_bid, id_user)) {
							delta = (-1) * delta;
						}
						temp_balance_update[j].balance_account = temp_balance_update[j].balance_account + delta;
					}
					temp_balance_update[j].t_update_balance = Platform.market_results_total[i].t_cleared;
				}
			}
		}
		return temp_balance_update;
	}
	//update the balances of the user infos in the storage, given the total market results on storage as well. It writes the results on storage
	function updateBalances() public {
		for(uint i = 0; i < Platform.market_results_total.length; i++) {
			int delta = int(Platform.market_results_total[i].price_energy_market_uniform * Platform.market_results_total[i].qty_energy_traded);//I don't divide by 1000, since there is no float
			for(uint j = 0; j < Platform.user_infos.length; j++) {
				if(delta < 0) delta = (-1) * delta;
				string memory id_user =Platform.user_infos[j].id_user;
				if(lib.compareStrings(Platform.market_results_total[i].id_user_offer, id_user) || lib.compareStrings(Platform.market_results_total[i].id_user_bid, id_user)) {
					if(!(lib.compareStrings(Platform.market_results_total[i].id_user_offer,Platform.market_results_total[i].id_user_bid))) {
						if(lib.compareStrings(Platform.market_results_total[i].id_user_bid, id_user)) {
							delta = (-1) * delta;
						}
						Platform.user_infos[j].balance_account = Platform.user_infos[j].balance_account + delta;
					}
					Platform.user_infos[j].t_update_balance = Platform.market_results_total[i].t_cleared;
				}
			}
		}
	}
	//it performs the full market clearing. The results are then stored in the variable market_results_total
	function market_clearing(uint n_clearings, uint t_clearing_first, bool supplier_bids, bool uniform_pricing, bool discriminative_pricing, uint clearing_interval, uint t_clearing_start, bool shuffle, bool verbose, bool update_balances, bool simulation_test) public {
	    if(verbose) {
	    	string_to_log = lib.concatenateStrings("Market clearing started on the blockchain\nNumber of clearings: ",lib.uintToString(n_clearings));
	    	string_to_log = lib.concatenateStrings(string_to_log,"\n");//two statements to reduce stack usage
	    }
	    for (uint i = 0; i < n_clearings; i++) {
            //Continuous clearing time, incrementing by market period
        	uint t_clearing_current = t_clearing_first + clearing_interval * i;
        	if(verbose) {
        		string_to_log = lib.concatenateStrings(string_to_log, lib.concatenateStrings("Clearing number: ",lib.uintToString(i)));
        		string_to_log = lib.concatenateStrings(string_to_log, lib.concatenateStrings(lib.concatenateStrings(". t_clearing_current = ", lib.uintToString(t_clearing_current)),".\n"));
        	}
        	bool add_supplier_bids;
        	if (i == 0 && supplier_bids) add_supplier_bids = supplier_bids;
        	else add_supplier_bids = false;
        	//performing a single clearing
        	single_clearing(t_clearing_current, add_supplier_bids, uniform_pricing, discriminative_pricing, t_clearing_start, false, true, verbose, shuffle, simulation_test);
    	}
    	//updating balances on storage
    	if(update_balances) updateBalances();
    	if(verbose) {
    		string_to_log = lib.concatenateStrings(string_to_log, "Updated balances of users");
    		emit logString(string_to_log);
    	}
	}
	function determine_balancing_energy(uint[] list_ts_delivery){
		Lb.Lib.market_result[] memory sorted_results=srt.quick_sort_market_result_ts_delivery(Platform.temp_market_results, true);
		for(uint i=0; i<list_ts_delivery.length-1; i++){
			Lb.Lib.meter_reading_delta[] memory meters=lib.meters_delta_inside_ts_delivery(Platform.meter_reading_deltas, list_ts_delivery[i]);
			Lb.Lib.market_result[] memory results=lib.market_results_inside_ts_delivery(sorted_results, list_ts_delivery[i]);
			for(uint j=0; j<meters.length-1;j++){
				uint current_actual_energy=meters[j].energy_out-meters[j].energy_in;
				uint current_market_energy=0;
				for(k=0; k<results.length-1;k++){
					if(lib.compareStrings(meters[j].id_meter, results[k].id_user_bid)){
						current_market_energy -=  results[k].qty_energy_traded;
					}
					else if(lib.compareStrings(meters[j].id_meter, results[k].id_user_offer)){
						current_market_energy += results[k].qty_energy_traded;
					}
				}
				current_actual_energy -= current_market_energy;
				Lb.Lib.energy_balancing result_energy;
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
				Platform.energy_balances.push(result_energy);
			}

		}
	}
}