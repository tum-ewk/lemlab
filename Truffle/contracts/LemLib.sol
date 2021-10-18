pragma solidity >=0.5.0 <0.7.5;
pragma experimental ABIEncoderV2;

contract LemLib {

    struct offer_bid {
            string id_user;
            uint qty_energy;
            uint price_energy;
            uint quality_energy;
            uint premium_preference_quality;
            string type_position;
            uint number_position;
            uint status_position;
            uint t_submission;
            uint ts_delivery;
       }
    struct market_result {
        string id_user_offer;
        uint qty_energy_offer;
        uint price_energy_offer;
        uint quality_energy_offer;
        uint premium_preference_quality_offer;
        string type_position_offer;
        uint number_position_offer;
        uint status_position_offer;
        uint t_submission_offer;
        uint ts_delivery;
        string id_user_bid;
        uint qty_energy_bid;
        uint price_energy_bid;
        uint quality_energy_bid;
        uint premium_preference_quality_bid;
        string type_position_bid;
        uint number_position_bid;
        uint status_position_bid;
        uint t_submission_bid;
        uint price_energy_market_uniform;
        uint price_energy_market_discriminative;
        uint qty_energy_traded;
        uint share_quality_NA;
        uint share_quality_local;
        uint share_quality_green;
        uint share_quality_green_local;
    }
    struct market_result_total {
        string id_user_offer;
        uint price_energy_offer;
        uint number_position_offer;
        uint ts_delivery;
        string id_user_bid;
        uint price_energy_bid;
        uint number_position_bid;
        uint price_energy_market_uniform;
        uint price_energy_market_discriminative;
        uint qty_energy_traded;
        uint share_quality_NA;
        uint share_quality_local;
        uint share_quality_green;
        uint share_quality_green_local;
        uint t_cleared;
    }
    struct user_info {
            string id_user;
            int balance_account;
            uint t_update_balance;
            uint price_energy_bid_max;
            uint price_energy_offer_min;
            string preference_quality;
            uint premium_preference_quality;
            string strategy_market_agent;
            uint horizon_trading;
            string id_market_agent;
            uint ts_delivery_first;
            uint ts_delivery_last;
    }
    struct id_meter {
        string id_meter;
        string id_user;
        string id_meter_super;       // (order changed)
        string type_meter;
        string id_aggregator;
        string quality_energy;
        uint ts_delivery_first;
        uint ts_delivery_last;
        string info_additional;
    }
    struct meter_reading_cum {
        uint t_reading;
        string id_meter;
        uint energy_in_cum;
        uint energy_out_cum;
    }
    struct meter_reading_delta {
        uint ts_delivery;
        string id_meter;
        uint energy_in;
        uint energy_out;
    }
    struct energy_balancing {
        string id_meter;
        uint ts_delivery;
        uint energy_balancing_positive;
        uint energy_balancing_negative;
    }
    struct price_settlement{
        uint ts_delivery;
        uint price_energy_balancing_positive;
        uint price_energy_balancing_negative;
        uint price_energy_levies_positive;
        uint price_energy_levies_negative;
    }
    struct log_transaction{
        string id_user;
        uint ts_delivery;
        uint price_energy_market;
        string type_transaction;
        int qty_energy;         // some values can be negative too
        int delta_balance;
        uint t_update_balance;
        uint64 share_quality_offers_cleared_na;
        uint64 share_quality_offers_cleared_local;
        uint64 share_quality_offers_cleared_green;
        uint64 share_quality_offers_cleared_green_local;
    }
    struct status_settlement{
        uint ts_delivery;
        uint16 status_meter_readings_processed;
        uint16 status_settlement_complete;
    }

    uint constant timestep_size = 15 * 60;                    // we use a timestep of 15 minutes, converted to seconds
    uint constant num_meters = 20;                            // fixed number of meters_delta
    uint constant horizon = 7*24*60*60/timestep_size;         // 7 days, divided in 15 minutes intervals
    uint euro_to_sigma=1e9;
    //functions for returning the variables
    function get_horizon()public pure returns(uint){
        return horizon;
    }
    function get_num_meters() public pure returns(uint){
        return num_meters;
    }
    function get_timestep() public pure returns(uint){
        return timestep_size;
    }
    //return true if a list of user infos has at least one user with id_user as the argument given in input
    function check_user_id_in_user_infos(string memory id_user, user_info[] memory user_infos) public pure returns(bool) {
        for(uint i = 0; i < user_infos.length; i++) {
            if(compareStrings(user_infos[i].id_user, id_user)) return true;
        }
        return false;
    }
    //same as check_user_id_in_user_infos. it also checks if ts_delivery is between ts_delivery_first and ts_delivery_last of the user
    function check_user_id_in_user_infos_interval(string memory id_user, uint ts_delivery, user_info[] memory user_infos) public pure returns(bool) {
        for(uint i = 0; i < user_infos.length; i++) {
            if(compareStrings(user_infos[i].id_user, id_user)) {
                return (user_infos[i].ts_delivery_first <= ts_delivery && ts_delivery <= user_infos[i].ts_delivery_last);
            }
        }
        return false;
    }
    //return true if the two strings given in input have the same value
    function compareStrings(string memory a, string memory b) public pure returns (bool) {
        return (keccak256(abi.encode((a))) == keccak256(abi.encode((b))));
    }
    //converts a string to bytes32
    function stringToBytes32(string memory source) public pure returns (bytes32 result) {
        bytes memory tempEmptyStringTest = bytes(source);
        if (tempEmptyStringTest.length == 0) {
            return 0x0;
        }

        assembly {
            result := mload(add(source, 32))
        }
    }
    //compares two pairs of numbers and returns a boolean according if one wants ascending/descending sorting
    function compare_two_keys(uint val_one_first, uint val_two_first, uint val_one_second, uint val_two_second, bool ascending_first, bool ascending_second) public pure returns(bool) {
        if (val_one_first != val_two_first) {
            if (ascending_first)  return val_two_first > val_one_first;
            else return val_two_first < val_one_first;
        }
        else {
            if (val_two_second == val_one_second) return true;
            if (ascending_second) return val_two_second > val_one_second;
            else return val_two_second < val_one_second;
        }
    }
    //compares two triplets of numbers and returns a boolean according if one wants ascending/descending sorting
    function compare_three_keys(uint val_one_first, uint val_two_first, uint val_one_second, uint val_two_second, uint val_one_third, uint val_two_third, bool ascending_first, bool ascending_second, bool ascending_third) public pure returns(bool) {
        if (val_one_first != val_two_first) {
            if (ascending_first)  return val_two_first > val_one_first;
            else return val_two_first < val_one_first;
        }
        else {
            if (val_one_second == val_two_second) {
                if (val_one_third == val_two_third) return true;
                if (ascending_third) return val_two_third > val_one_third;
                else return val_two_third < val_one_third;
            }
            if (ascending_second) return val_two_second > val_one_second;
            else return val_two_second < val_one_second;
        }
    }
    //copies an array of uint and its values from index start to index end
    function copyArray(uint[] memory arr, uint start, uint end) public pure returns(uint[] memory) {
        uint[] memory copy = new uint[](arr.length);
        for(uint i = start; i <= end; i++) {
            copy[i] = arr[i];
        }
        return copy;
    }
    //copies an array of user_info and its values from index start to index end
    function copyArray_UserInfo(user_info[] memory arr, uint start, uint end) public pure returns(user_info[] memory) {
        user_info[] memory copy = new user_info[](arr.length);
        for(uint i = start; i <= end; i++) {
            copy[i] = arr[i];
        }
        return copy;
    }
    //returns a new array, equal to the one in input, but with a position more in correspondence of the index start
    function add_pos(uint[] memory arr, uint start) public pure returns(uint[] memory){
        uint[] memory new_arr = new uint[](arr.length + 1);
        for(uint i = 0; i < start; i++) {
            new_arr[i] = arr[i];
        }
        for(uint i = start; i < arr.length; i++) {
            new_arr[i+1] = arr[i];
        }

        return new_arr;
    }
    //sums the values of a uint array
    function sumArrIndices(uint start, uint end, uint[] memory arr) public pure returns (uint) {
        uint s;

        for(uint i = start;i <= end; i ++){
            s += arr[i];
        }
        return s;
    }
    /*
    takes a list of offer_bid as an input.
    starting from position 0, to the end of the list, the energy cumulated array is calculated as a sum of the quantity of energy and then returned
    */
    function getEnergyCumulated(offer_bid[] memory offers_bids) public pure returns (uint[] memory) {
        uint[] memory energy_cumulated = new uint[](offers_bids.length);

        for (uint i = 0; i < energy_cumulated.length; i++) {
            energy_cumulated[i] = sumArrIndices(0, i, arr_of_quantities_offerbids(offers_bids));
        }
        return energy_cumulated;
    }
    //converts uint to string
    function uintToString(uint v) public pure returns (string memory) {
        if(v==0){
            return "0";
        }
        uint maxlength = 100;
        bytes memory reversed = new bytes(maxlength);
        uint i = 0;
        while (v != 0) {
            uint remainder = v % 10;
            v = v / 10;
            reversed[i++] = byte(uint8(48 + remainder));
        }
        bytes memory s = new bytes(i + 1);
        for (uint j = 0; j <= i; j++) {
            s[j] = reversed[i - j];
        }
        return string(s);
    }
    //merges two strings into one string
    function concatenateStrings(string memory a, string memory b) public pure returns (string memory) {
        return string(abi.encodePacked(a, b));
    }
    //converts a uint to string, and then appends it to another string, given in input
    function appendUintToString(string memory inStr, uint v) public pure returns (string memory) {
        uint maxlength = 100;
        bytes memory reversed = new bytes(maxlength);
        uint i = 0;
        while (v != 0) {
            uint remainder = v % 10;
            v = v / 10;
            reversed[i++] = byte(uint8(48 + remainder));
        }
        bytes memory inStrb = bytes(inStr);
        bytes memory s = new bytes(inStrb.length + i + 1);
        uint j;
        for (j = 0; j < inStrb.length; j++) {
            s[j] = inStrb[j];
        }
        for (j = 0; j <= i; j++) {
            s[j + inStrb.length] = reversed[i - j];
        }
        return string(s);
    }
    //converts a string to uint
    function stringToUint(string memory s) public pure returns (uint result) {
        bytes memory b = bytes(s);
        uint i;
        result = 0;
        for (i = 0; i < b.length; i++) {
            uint8 c = uint8(b[i]);
            if (c >= 48 && c <= 57) {
                result = result * 10 + (c - 48);
            }
        }
    }
    /*computes and return an array of differences of the lenght as the input array -1.
    For every position of the new array, every element of the new array is equal to
    the difference between the element at the next position in the input array, and the element at the position in the input array
    */
    function computeDifferences(uint[] memory arr, uint start, uint end) public pure returns(uint[] memory) {
        uint[] memory differences = new uint[](end - start);

        for(uint i = start; i < end;i++) {
            differences[i] = arr[i+1] - arr[i];
        }

        return differences;
    }
    //FUNCTIONS FOR OFFER OR BIDS
    //gets the array of ts_deliveries from an array of offer_bid
    function arr_of_ts_deliveries_offerbids(offer_bid[] memory offerbid) public pure returns(uint[] memory) {
        uint[] memory arr_ts_deliveries = new uint[](offerbid.length);
        for (uint i=0; i<arr_ts_deliveries.length;i++) {
            arr_ts_deliveries[i]=offerbid[i].ts_delivery;
        }
        return arr_ts_deliveries;
    }
    //gets the array of quantities from an array of offer_bid
    function arr_of_quantities_offerbids(offer_bid[] memory offerbid) public pure returns(uint[] memory) {
        uint[] memory arr_quantities = new uint[](offerbid.length);
        for (uint i = 0; i < arr_quantities.length;i++) {
            arr_quantities[i] = offerbid[i].qty_energy;
        }
        return arr_quantities;
    }
    //gets the array of qualities from an array of offer_bid
    function arr_of_qualities_offerbids(offer_bid[] memory offerbid) public pure returns(uint[] memory) {
        uint[] memory arr_qualities = new uint[](offerbid.length);
        for (uint i = 0; i < arr_qualities.length;i++) {
            arr_qualities[i] = offerbid[i].quality_energy;
        }
        return arr_qualities;
    }
    //concatenates two arrays of offer_bid into a new array and return it
    function concatenateOffersBids(offer_bid[] memory one, offer_bid[] memory two) public pure returns (offer_bid[] memory) {
        offer_bid[] memory concat = new offer_bid[](one.length + two.length);
        for(uint i = 0; i < one.length; i++) {
            concat[i] = one[i];
        }
        for(uint i = one.length; i < concat.length; i++) {
            concat[i] = two[i];
        }
        return concat;
    }
    //gets the array of qty_energy_traded from an array of market_result
    function arr_of_energy_cumulated_merged_offers_bids(market_result[] memory mr) public pure returns(uint[] memory) {
        uint[] memory arr_energies = new uint[](mr.length);
        for (uint i = 0; i < arr_energies.length; i++) {
            arr_energies[i] = mr[i].qty_energy_traded;
        }
        return arr_energies;
    }
    //gets the array of prices from an array of offer_bid
    function arr_of_prices_offerbids(offer_bid[] memory offerbid) public pure returns(uint[] memory) {
        uint[] memory arr_prices = new uint[](offerbid.length);
        for (uint i = 0; i < arr_prices.length;i++) {
            arr_prices[i] = offerbid[i].price_energy;
        }
        return arr_prices;
    }
    //check if an array of offer_bid is sorted
    function checkSortedTsDelivery(offer_bid[] memory offerbid) public pure returns(bool) {
        for(uint i = 0; i < offerbid.length-1; i++) {
            if(offerbid[i].ts_delivery>offerbid[i+1].ts_delivery) {
                return false;
            }
        }
        return true;
    }
    //FUNCTIONS FOR MARKET RESULTS
    //returns the ts_deliveries of an array
    function arr_of_ts_deliveries_market_result(market_result[] memory results) public pure returns(uint[] memory) {
        uint[] memory arr_ts_deliveries = new uint[](results.length);
        for (uint i=0; i<arr_ts_deliveries.length;i++) {
            arr_ts_deliveries[i]=results[i].ts_delivery;
        }
        return arr_ts_deliveries;
    }
    //function to get the market results inside a ts_delivery, it first copies the results, then deletes the rest of
    //the entries not used, this is done because memory arrays cannot be dynamic and we dont need a storage array for this
    function market_results_inside_ts_delivery(market_result_total[] memory results, uint ts_delivery) public pure returns(market_result_total[] memory){
        uint length_results=0;
        for(uint i=0; i<results.length; i++){
            if(results[i].ts_delivery==ts_delivery){
                length_results++;
            }
        }
        if(length_results==0){
            market_result_total[] memory sample=new market_result_total[](1);
            sample[0].ts_delivery=uint(-1);
            return sample;
        }
        market_result_total[] memory filtered_results= new market_result_total[](length_results);
        uint index=0;
        for(uint i=0; i<results.length; i++){
            if(results[i].ts_delivery==ts_delivery){
                filtered_results[index]=results[i];
                index++;
            }
        }
        return filtered_results;
    }
    //FUNCTIONS FOR DELTA METERS
    //function to get the meter readings inside a ts_delivery, memory optimized. It needs to first read the amount of
    // meters that there are to create the array, since memory arrays are not allowed to be dynamically changed once
    // initialized
    function meters_delta_inside_ts_delivery(meter_reading_delta[] memory meters, uint ts_delivery) public pure returns(meter_reading_delta[] memory){
        uint length_meters=0;
        for(uint i=0; i<meters.length; i++){
            if(meters[i].ts_delivery==ts_delivery){
                length_meters++;
            }
        }
        if(length_meters==0){
            meter_reading_delta[] memory sample= new meter_reading_delta[](1);
            sample[0].ts_delivery=uint(-1);
            return sample;
        }
        meter_reading_delta[] memory filtered_results= new meter_reading_delta[](length_meters);
        uint index=0;
        for(uint i=0; i<meters.length; i++){
            if(meters[i].ts_delivery==ts_delivery){
                filtered_results[index]=meters[i];
                index++;
            }
        }
        return filtered_results;
    }
    function ts_delivery_to_index(uint ts_delivery) public pure returns(uint){
        uint monday_00 = 1626040800;    //reference unix time from Monday 12th July 2021 at 00:00 at Berlin timezone
        uint dist=horizon*timestep_size+1;
        uint div=(ts_delivery-monday_00)/dist;
        // we transform first the ts into a ts inside a week time starting from monday_00
        // by moving it into the same week
        ts_delivery=ts_delivery-div*dist;
        // we then calculate the index based on the distance, being monday_00 the 0 up to 672
        uint index = ts_delivery%monday_00;
        index = index / timestep_size;
        return index;

    }
    // MATH UTILITY FUNCTIONS
    //returns the max element of an array
    function maxArray(uint[] memory arr) public pure returns(uint) {
        uint highest = 0;
            for(uint i = 0; i < arr.length; i++){
                if(arr[i] > highest) {
                    highest = arr[i];
                }
            }
        return highest;
    }
    //returns the min element of an array
    function minArray(uint[] memory arr) public pure returns(uint) {
        uint lowest = arr[0];
        for(uint i = 0; i < arr.length; i++){
            if(arr[i] < lowest) {
                lowest = arr[i];
            }
        }
        return lowest;
    }
    //reverse the order of a uint array
    function reverseArray(uint[] memory arr, uint start, uint end) public pure returns(uint[] memory) {
        uint[] memory rev = new uint[](arr.length);
        for(uint i = 0;i < arr.length;i++) {
            if(i < start || i > end) rev[i] = arr[i];
            else {
                rev[i] = arr[end-(i-start)];
            }
        }
        return rev;
    }
    //returns a uint array of length = end - start + 1, with the elements of the given array from start to end(included)
    function cropArray(uint[] memory data, uint start, uint end) public pure returns(uint[] memory){
        uint[] memory cropped = new uint[](end -  start +1);
        uint z = 0;
        for(uint i = start; i <= end; i++) {
            cropped[z] = data[i];
            z++;
        }
        return cropped;
    }
    //returns a offer_bid array of length = end - start + 1, with the elements of the given array from start to end(included)
    function cropOfferBids(offer_bid[] memory data, uint start, uint end) public pure returns(offer_bid[] memory){
        offer_bid[] memory cropped = new offer_bid[](end -  start +1);
        uint z = 0;
        for(uint i = start; i <= end; i++) {
            cropped[z] = data[i];
            z++;
        }
        return cropped;
    }
    /*returns an array of arrays of indices. This has the lenght of the data interval(eg [100,1000] = 901).
    For every position, it saves all the indices of the data where the element is equal to the value in the interval
    */
    function getCountIndices(uint[] memory count, uint[] memory data, uint[] memory indices, uint start, uint end) public pure returns(uint[][] memory) {
        //count size = max-min+1 (of cropped_data)
        //data size = full data.length(no start/end)
        //indices size = full data.length(no start/end)
        uint[] memory data_cropped = cropArray(data, start, end);
        uint min = minArray(data_cropped);

        uint z;
        uint[][] memory count_indices = new uint[][](count.length);
        for (uint i = 0; i < count.length; i++) {
            count_indices[i] = new uint[](count[i]);
            z = 0;
            uint val = i + min;
            for(uint j = start; j <= end; j++) {
                if(data[j] == val) {
                    count_indices[i][z] = indices[j];
                    z++;
                }
            }
        }
        return count_indices;
    }
    /*
    For insertionsort. given two sorted arrays until a certain position end_pos, and an element at a position new_element_ind, it finds the new position at which the new element should be placed
    */
    function findPosition_new_element_sort(uint end_pos, uint new_element_ind, uint[] memory arr_first, uint[] memory arr_second, bool ascending_first, bool ascending_second) public pure returns(uint) {
        uint current_pos = end_pos;
        uint new_element_first = arr_first[new_element_ind];
        uint new_element_second = arr_second[new_element_ind];
        bool go_on = true;
        uint new_ind;
        while(go_on && current_pos >= 0) {
            if (compare_two_keys(arr_first[current_pos], new_element_first, arr_second[current_pos], new_element_second, ascending_first, ascending_second)) {
                //if ascending, true if new_element_first>arr_first[current_pos]
                new_ind = current_pos + 1;
                go_on = false;
            }
            else {
                if(current_pos == 0) return 0;
                current_pos --;
            }
        }
        return new_ind;
    }
    /*
    moves an element of an array from an index ind_two to a certain position ind_one. move all the elements with the index <= ind_two and > ind_one to the right
    example [0,2,1,3,5,4,6,8,7] ind_two = 7, ind_one = 2 [0,2,8,1,3,5,4,6,7]
    */
    function slice_elements_arr(uint[] memory arr, uint ind_one, uint ind_two) public pure returns (uint[] memory) {
        uint temp = arr[ind_two];
        for(uint i = ind_two; i > ind_one; i--) {
            arr[i] = arr[i-1];
        }
        arr[ind_one] = temp;
        return arr;
    }
    //swap the elements of one array at two indices
    function swap_elements_arr(uint[] memory arr, uint ind_one, uint ind_two) public pure returns(uint[] memory) {
        uint temp = arr[ind_one];
        arr[ind_one] = arr[ind_two];
        arr[ind_two] = temp;
        return arr;
    }
    //get the list of indices, given a certain length e.g. len = 4 returns [0,1,2,3]
    function getIndices(uint len) public pure returns(uint[] memory){
        uint[] memory indices = new uint[](len);
        for (uint i = 0; i < indices.length; i++) {
            indices[i] = i;
        }
        return indices;
    }
    //normalize an array. it calculates the min value and subtract it from all the elements of the array
    function normalizeArr(uint[] memory data) public pure returns(uint[] memory) {
        uint min = minArray(data);
        for (uint i = 0; i < data.length; i++) {
            data[i] = data[i]-min;
        }
        return data;
    }
    /*get the count array from a uint array. the count array has length equal to the interval of the data.
    it describes how many times every value in the interval is present in the data array
    */
    function getCount(uint[] memory data) public pure returns(uint[] memory) {
        uint max = maxArray(data);
        uint min = minArray(data);
        uint[] memory count = new uint[](max - min + 1);
        for (uint i = 0; i < count.length; i++) {
            count[i] = 0;
        }
        for (uint i = 0; i < data.length; i++) {
            count[data[i] - min]++;
        }

        return count;
    }
    //reorders a uint array from start to end index, accoording to new indices
    function reorderArr(uint[] memory new_indices, uint[] memory arr, uint start, uint end) public pure returns(uint[] memory) {
        uint[] memory reordered = new uint[](arr.length);
        for (uint i = start; i <= end; i++) {
            reordered[i] = arr[new_indices[i]];
        }
        return reordered;
    }
    //shuffles an array of uint
    function shuffle_arr(uint[] memory arr) public view returns(uint[] memory) {
        for (uint i = 0; i < arr.length; i++) {
            uint n = i + uint(keccak256(abi.encodePacked(block.timestamp))) % (arr.length - i);
            uint temp = arr[n];
            arr[n] = arr[i];
            arr[i] = temp;
        }
        return arr;
    }
    //shuffles an array of offer_bid
    function shuffle_OfferBids(offer_bid[] memory offers_bids) public view returns(offer_bid[] memory) {
        uint[] memory indices_offerbisd = getIndices(offers_bids.length);
        uint[] memory shuffled_indices = shuffle_arr(indices_offerbisd);
        offer_bid[] memory shuffled_offers_bids = reorderArr_OfferBid(shuffled_indices, offers_bids);
        return shuffled_offers_bids;
    }
    //reorders an array of offer_bid from start to end index, accoording to new indices
    function reorderArr_OfferBid(uint[] memory new_indices, offer_bid[] memory arr) public pure returns(offer_bid[] memory) {
        offer_bid[] memory reordered = new offer_bid[](arr.length);
        for (uint i = 0; i < new_indices.length; i++) {
            reordered[i] = arr[new_indices[i]];
        }
        return reordered;
    }
    //finds how many times a value is present in an array of uint
    function find_num_same_value(uint[] memory data, uint value, bool sorted) public pure returns(uint) {
        uint count = 0;
        uint j;
        if(sorted) {
            uint i = 0;
            while(i < data.length) {
                if(data[i] == value) {
                    j = i;
                    while(j < data.length && data[j] == value) {
                        count++;
                        j++;
                    }
                    i = data.length;
                }
                i++;
            }
        }
        else {
            for(uint i = 0; i < data.length; i++) {
                if(data[i] == value) count++;
            }
        }
        return count;
    }
}