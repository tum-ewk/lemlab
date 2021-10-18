pragma solidity >=0.5.0 <0.7.5;
pragma experimental ABIEncoderV2;

import "./ClearingExAnte.sol" as Pl;
import "./LemLib.sol" as Lb;

contract Sorting {
    Lb.LemLib lib = new Lb.LemLib();
    event logString(string arg);
    
    //perform the quicksort over an array, given in input. it doesn't return anything since it modifies the given input inside
    //left and right are mark the start and the end index in between which, the quicksort is performed.
    //it is a recursive algorithm
    function quickSort(uint[] memory arr, int left, int right) public pure {
	    int i = left;
	    int j = right;
	    if (i == j) return;
	    uint pivot = arr[uint(left + (right - left) / 2)];
	    while (i <= j) {
	        while (arr[uint(i)] < pivot) i++;
	        while (pivot < arr[uint(j)]) j--;
	        if (i <= j) {
	            (arr[uint(i)], arr[uint(j)]) = (arr[uint(j)], arr[uint(i)]);
	            i++;
	            j--;
	        }
	    }
	    if (left < j)
	        quickSort(arr, left, j);
	    if (i < right)
	        quickSort(arr, i, right);
	}

	//same as quicksort(). Also, it modifies the list of indices of the array.
	//It can be useful if with these new indices, one wants to reorder a second array
	function quickSort_indices(uint[] memory arr, int left, int right, uint[] memory indices) public pure {
	    int i = left;
	    int j = right;
	    if (i == j) return;

	    uint pivot = arr[uint(left + (right - left) / 2)];

	    while (i <= j) {
            while (arr[uint(i)] < pivot) i++;
	        while (pivot < arr[uint(j)]) j--;
	        if (i <= j) {
	            (arr[uint(i)], arr[uint(j)]) = (arr[uint(j)], arr[uint(i)]);
	            (indices[uint(i)], indices[uint(j)]) = (indices[uint(j)], indices[uint(i)]);
	            i++;
	            j--;
	        }
	    }
	    if (left < j)
	        quickSort_indices(arr, left, j, indices);
	    if (i < right)
	        quickSort_indices(arr, i, right, indices);
	}
	//same as quickSort_indices(). it performs the quicksort over two arrays. the first array as priority
	function quickSort_indices_two_arr(uint[] memory arr_first, uint[] memory arr_second, int left, int right, uint[] memory indices, bool ascending_first, bool ascending_second) public pure {
	    int i = left;
	    int j = right;
	    if (i == j) return;

	    int pivot_ind = left + (right - left) / 2;
	    uint pivot = arr_first[uint(pivot_ind)];
	    uint pivot_second = arr_second[uint(pivot_ind)];
	    
	    while (i <= j) {
	        if(ascending_first) {
	            if(ascending_second) {
	                while (arr_first[uint(i)] < pivot || (arr_first[uint(i)] == pivot && arr_second[uint(i)] < pivot_second)) i++;
	        	    while (pivot < arr_first[uint(j)] || (arr_first[uint(j)] == pivot && arr_second[uint(pivot_ind)] < arr_second[uint(j)])) j--;
	            }
	            else{
	                while (arr_first[uint(i)] < pivot || (arr_first[uint(i)] == pivot && arr_second[uint(i)] > pivot_second)) i++;
	        	    while (pivot < arr_first[uint(j)] || (arr_first[uint(j)] == pivot && arr_second[uint(pivot_ind)] > arr_second[uint(j)])) j--;
	            }
	        }
	        else {
	            if(ascending_second) {
	                while (arr_first[uint(i)] > pivot || (arr_first[uint(i)] == pivot && arr_second[uint(i)] < pivot_second)) i++;
	        	    while (pivot > arr_first[uint(j)] || (arr_first[uint(j)] == pivot && arr_second[uint(pivot_ind)] < arr_second[uint(j)])) j--;
	            }
	            else{
	                while (arr_first[uint(i)] > pivot || (arr_first[uint(i)] == pivot && arr_second[uint(i)] > pivot_second)) i++;
	        	    while (pivot > arr_first[uint(j)] || (arr_first[uint(j)] == pivot && arr_second[uint(pivot_ind)] > arr_second[uint(j)])) j--;
	            }
	        }
	        if (i <= j) {
	            (arr_first[uint(i)], arr_first[uint(j)]) = (arr_first[uint(j)], arr_first[uint(i)]);
	            (arr_second[uint(i)], arr_second[uint(j)]) = (arr_second[uint(j)], arr_second[uint(i)]);
	            (indices[uint(i)], indices[uint(j)]) = (indices[uint(j)], indices[uint(i)]);
	            i++;
	            j--;
	        }
	    }
	    if (left < j)
	        quickSort_indices_two_arr(arr_first, arr_second, left, j, indices, ascending_first, ascending_second);
	    if (i < right)
	        quickSort_indices_two_arr(arr_first, arr_second, i, right, indices, ascending_first, ascending_second);
	}
	//sorts an array using the countingsort. it returns the sorted array
	function countingSort(uint[] memory data, bool ascending) public view returns(uint[] memory){
	    uint max = lib.maxArray(data);
	    uint min = lib.minArray(data);
	    uint[] memory sorted = new uint[](data.length);
	    uint[] memory count = new uint[](max-min);
	    for (uint i = 0; i < data.length; i++) {
	        data[i]=data[i]-min;
	        count[data[i]]++;
        }
        uint j=0;
        if(ascending) {
            for (uint i = 0; i < count.length; i++) {
                while (count[i] > 0) {
                    sorted[j] = count[i];
                    j++;
                }
            }
        }
        else {
            for (uint i = count.length-1; i >=0; i--) {
                while (count[i] > 0) {
                    sorted[j]=count[i];
                    j++;
                }
            }
        }
        
        return sorted;
	}
	//sorts an array using the countingsort. It doesn't return since the array is already modified inside
	function countingSort_void(uint[] memory data, uint setSize) public pure {
        uint[] memory set = new uint[](setSize);
        for (uint i = 0; i < data.length; i++) {
            set[data[i]]++;
        }
        uint j = 0;
        for (uint i = 0; i < setSize; i++) {
            while (set[i]-- > 0) {
                data[j] = i;
                if (++j >= data.length) break;
            }
        }
    }
	//same as countingSort() but it returns the indices of the sorted array
	function countingSort_indices(uint[] memory data, bool ascending, uint start, uint end) public view returns(uint[] memory){
	    uint[] memory data_cropped = lib.cropArray(data, start, end);
	    //data_cropped = normalizeArr(data_cropped);
	    
	    uint[] memory indices = lib.getIndices(data.length);
	    
	    uint[] memory sorted_indices = new uint[](indices.length);
	    for (uint i = 0; i < sorted_indices.length; i++) {
            sorted_indices[i] = indices[i];
	    }
	    
	    uint[] memory count = lib.getCount(data_cropped);
	    
        //count size = max-min+1
        //data size = full data.length(no start/end)
        //indices size = full data.length(no start/end)
        uint[][] memory count_indices = lib.getCountIndices(count, data, indices, start, end);
        //count_indices size = max-min+1 data cropped
        
        uint z = start;
        if(ascending) {
            for(uint i = 0; i < count_indices.length; i++){
                for(uint j = 0; j < count_indices[i].length; j++) {
                    sorted_indices[z] = count_indices[i][j];
                    z++;
                }
            }
        }
        else {
            for(int i = int(count_indices.length-1); i >= 0; i--){
                for(uint j = 0; j < count_indices[uint(i)].length; j++) {
                    sorted_indices[z] = count_indices[uint(i)][j];
                    z++;
                }
            }
        }
        
        return sorted_indices;
	}
	//using countingsort, it sorts an array of integers, then it sorts an array of offer_bid based on the same sorting and returns it.
	function get_indices_and_sort_countingsort(uint[] memory values, Lb.LemLib.offer_bid[] memory offers_bids, bool ascending) private view returns(Lb.LemLib.offer_bid[] memory) {
		uint[] memory sorted_indices = Sorting.countingSort_indices(values, ascending, 0, values.length-1);
		Lb.LemLib.offer_bid[] memory sorted = new Lb.LemLib.offer_bid[](values.length);
		for (uint i = 0; i < sorted_indices.length; i++) {
            sorted[i] = offers_bids[sorted_indices[i]];
	    }
	    return sorted;
	}
	//same as get_indices_and_sort_countingsort(), but it sorts using two arrays(two keys)
	function get_indices_and_sort_countingsort_two_arr(uint[] memory values_first, uint[] memory values_second, Lb.LemLib.offer_bid[] memory offers_bids, bool ascending_first, bool ascending_second) public view returns(Lb.LemLib.offer_bid[] memory) {
		uint[] memory sorted_indices = Sorting.countingSort_indices(values_first, ascending_first, 0, values_first.length-1);
		
		uint[] memory sorted_first = lib.reorderArr(sorted_indices, values_first, 0, values_first.length-1);
		uint[] memory reordered_second = lib.reorderArr(sorted_indices, values_second, 0, values_second.length-1);
		
	    sorted_indices = countingsort_by_second_value_indices(sorted_indices, sorted_first, reordered_second, ascending_second);
	    
	    Lb.LemLib.offer_bid[] memory sorted_offers_bids = lib.reorderArr_OfferBid(sorted_indices, offers_bids);

	    return sorted_offers_bids;
	}
	//sorts a second array, in case of same value in a first array
	function countingsort_by_second_value_indices(uint[] memory sorted_indices, uint[] memory sorted_first, uint[] memory values_second, bool ascending) public view returns(uint[] memory){
	    uint[] memory indices = new uint[](sorted_indices.length);
	    for (uint i = 0; i < indices.length; i++) {
            indices[i] = sorted_indices[i];
	    }
	    
	    uint i = 0;
	    uint count = 0;
	    while (i < sorted_first.length) {
	        count = lib.find_num_same_value(sorted_first, sorted_first[i], true);
	        if(count > 1) {
	            uint start = i;
	            uint end = i + count - 1;
	            indices = sort_and_reorder_arr(values_second, ascending, start, end, indices);
	        }
	        i = i + count;
	    }
        return indices;
	}
	//it reorders an array based on the sorting done on another array. the sorting is done using the countingsort
	function sort_and_reorder_arr(uint[] memory data, bool ascending, uint start, uint end, uint[] memory arr) public view returns(uint[] memory) {
	    uint[] memory modified_indices = countingSort_indices(data, ascending, start, end);
	    uint[] memory reordered_arr = lib.reorderArr(modified_indices, arr, 0, arr.length-1);
	    return reordered_arr;
	}
	//using quicksort, it sorts an array of integers, then it sorts an array of offer_bid based on the same sorting and returns it.
	function get_indices_and_sort_quicksort(uint[] memory values, Lb.LemLib.offer_bid[] memory offers_bids, bool ascending) private view returns(Lb.LemLib.offer_bid[] memory) {
	    uint[] memory indices = new uint[](values.length);
	    for (uint z = 0; z < indices.length; z++) {
            indices[z] = z;
	    }
		Sorting.quickSort_indices(values, 0, int(values.length-1), indices);
		if(!ascending){
		    indices = lib.reverseArray(indices, 0, indices.length - 1);
		}
		Lb.LemLib.offer_bid[] memory sorted = new Lb.LemLib.offer_bid[](values.length);
		for (uint z = 0; z < indices.length; z++) {
            sorted[z] = offers_bids[indices[z]];
	    }
	    return sorted;
	}

	//using quicksort, sorts a list of offer_bid by ts_delivery
	function quickSortOffersBidsTsDelivery(Lb.LemLib.offer_bid[] memory arr, bool ascending) public view returns(Lb.LemLib.offer_bid[] memory) {
		if(arr.length == 0) return arr;
		uint[] memory ts_deliveries = lib.arr_of_ts_deliveries_offerbids(arr);
		Lb.LemLib.offer_bid[] memory sorted = get_indices_and_sort_quicksort(ts_deliveries,arr,ascending);
		return sorted;
	}
	//using quicksort, sorts a list of offer_bid by price
	function quickSortOffersBidsPrice(Lb.LemLib.offer_bid[] memory offers_bids, bool ascending) public view returns(Lb.LemLib.offer_bid[] memory) {
		if(offers_bids.length == 0) return offers_bids;
		uint[] memory prices = lib.arr_of_prices_offerbids(offers_bids);
		Lb.LemLib.offer_bid[] memory sorted = get_indices_and_sort_quicksort(prices,offers_bids,ascending);
		return sorted;
	}
	//using quicksort, sorts a list of offer_bid by price and then quantity
	function quickSortOffersBidsPrice_Quantity(Lb.LemLib.offer_bid[] memory offers_bids, bool ascending_price,  bool ascending_quantity) public view returns(Lb.LemLib.offer_bid[] memory) {
		if(offers_bids.length == 0) return offers_bids;
		uint[] memory prices = lib.arr_of_prices_offerbids(offers_bids);
		uint[] memory quantities = lib.arr_of_quantities_offerbids(offers_bids);
		Lb.LemLib.offer_bid[] memory sorted = get_indices_and_sort_two_arr_quicksort(prices,quantities,offers_bids,ascending_price, ascending_quantity);
		return sorted;
	}
	//same as get_indices_and_sort_quicksort(), but it sorts using two arrays
	function get_indices_and_sort_two_arr_quicksort(uint[] memory values_first, uint[] memory values_second, Lb.LemLib.offer_bid[] memory offers_bids, bool ascending_price, bool ascending_quantity) private pure returns(Lb.LemLib.offer_bid[] memory) {
	    uint[] memory indices = new uint[](values_first.length);
	    for (uint z = 0; z < indices.length; z++) {
	        indices[z] = z;
	    }
		Sorting.quickSort_indices_two_arr(values_first, values_second, 0, int(values_first.length-1), indices, ascending_price, ascending_quantity);
		Lb.LemLib.offer_bid[] memory sorted = new Lb.LemLib.offer_bid[](values_first.length);

		for (uint z = 0; z < indices.length; z++) {
		    sorted[z] = offers_bids[indices[z]];
		}
	    return sorted;
	}
	//using countingsort, sorts a list of offer_bid by price
	function countingSortOffersBidsPrice(Lb.LemLib.offer_bid[] memory offers_bids, bool ascending) public view returns(Lb.LemLib.offer_bid[] memory) {
		if(offers_bids.length == 0) return offers_bids;
		uint[] memory prices = lib.arr_of_prices_offerbids(offers_bids);
		Lb.LemLib.offer_bid[] memory sorted = get_indices_and_sort_countingsort(prices,offers_bids,ascending);
		return sorted;
	}
	//using countingsort, sorts a list of offer_bid by price and then quantity
	function countingSortOffersBidsPriceQuantity(Lb.LemLib.offer_bid[] memory offers_bids, bool ascending_price, bool ascending_quantity) public view returns(Lb.LemLib.offer_bid[] memory) {
		if(offers_bids.length == 0) return offers_bids;
		uint[] memory prices = lib.arr_of_prices_offerbids(offers_bids);
		uint[] memory quantities = lib.arr_of_quantities_offerbids(offers_bids);
		Lb.LemLib.offer_bid[] memory sorted = get_indices_and_sort_countingsort_two_arr(prices, quantities, offers_bids, ascending_price, ascending_quantity);
		return sorted;
	}
	//perform insertion sort over two keys(i.e. arrays of values). return the corresponding indices
	function getInsertionSortIndices_two_keys(uint[] memory arr_first, uint[] memory arr_second, bool ascending_first, bool ascending_second) public view returns(uint[] memory) {
	    uint[] memory new_indices = new uint[](1);
		bool go_on;
		new_indices[0] = 0;
		for(uint i = 1; i < arr_first.length; i++) {
	        go_on = true;
        	if (lib.compare_two_keys(arr_first[i], arr_first[new_indices[0]], arr_second[i], arr_second[new_indices[0]], ascending_first, ascending_second)) {
	            new_indices = lib.add_pos(new_indices, 0);//shift right
	            new_indices[0] = i;
	            go_on = false;
        	}
	        else if(lib.compare_two_keys(arr_first[new_indices[new_indices.length - 1]], arr_first[i], arr_second[new_indices[new_indices.length - 1]], arr_second[i], ascending_first, ascending_second)) {
	            new_indices = lib.add_pos(new_indices, new_indices.length);//shift left
	            new_indices[new_indices.length-1] = i;
	            go_on = false;
	        }
	        if(go_on && lib.compare_two_keys(arr_first[new_indices[0]], arr_first[i], arr_second[new_indices[0]], arr_second[i], ascending_first, ascending_second) && lib.compare_two_keys(arr_first[i], arr_first[new_indices[new_indices.length - 1]], arr_second[i], arr_second[new_indices[new_indices.length - 1]], ascending_first, ascending_second)) {
	            go_on = true;
	            uint z = 0;
	            while(go_on) {
	                if(lib.compare_two_keys(arr_first[i], arr_first[new_indices[z]], arr_second[i], arr_second[new_indices[z]], ascending_first, ascending_second)) {
	                    new_indices = lib.add_pos(new_indices, z);
	                    new_indices[z] = i;
	                    go_on = false;
	                }
	                z++;
	            }
	        }
		}
		return new_indices;
	}
	//same as getInsertionSortIndices_two_keys. this is by three keys
	function getInsertionSortIndices_three_keys(uint[] memory arr_first, uint[] memory arr_second, uint[] memory arr_third, bool ascending_first, bool ascending_second, bool ascending_third) public view returns(uint[] memory) {
	    uint[] memory new_indices = new uint[](1);
		bool go_on;
		new_indices[0] = 0;
		for(uint i = 1; i < arr_first.length; i++) {
	        go_on = true;
        	if (lib.compare_three_keys(arr_first[i], arr_first[new_indices[0]], arr_second[i], arr_second[new_indices[0]], arr_third[i], arr_third[new_indices[0]], ascending_first, ascending_second, ascending_third)) {
	            new_indices = lib.add_pos(new_indices, 0);//shift right
	            new_indices[0] = i;
	            go_on = false;
        	}
	        else if(lib.compare_three_keys(arr_first[new_indices[new_indices.length - 1]], arr_first[i], arr_second[new_indices[new_indices.length - 1]], arr_second[i], arr_third[new_indices[new_indices.length - 1]], arr_third[i], ascending_first, ascending_second, ascending_third)) {
	            new_indices = lib.add_pos(new_indices, new_indices.length);//shift left
	            new_indices[new_indices.length-1] = i;
	            go_on = false;
	        }
	        if(go_on && lib.compare_three_keys(arr_first[new_indices[0]], arr_first[i], arr_second[new_indices[0]], arr_second[i], arr_third[new_indices[0]], arr_third[i], ascending_first, ascending_second, ascending_third) && lib.compare_three_keys(arr_first[i], arr_first[new_indices[new_indices.length - 1]], arr_second[i], arr_second[new_indices[new_indices.length - 1]], arr_third[i], arr_third[new_indices[new_indices.length - 1]], ascending_first, ascending_second, ascending_third)) {
	            go_on = true;
	            uint z = 0;
	            while(go_on) {
	                if(lib.compare_three_keys(arr_first[i], arr_first[new_indices[z]], arr_second[i], arr_second[new_indices[z]], arr_third[i], arr_third[new_indices[z]], ascending_first, ascending_second, ascending_third)) {
	                    new_indices = lib.add_pos(new_indices, z);
	                    new_indices[z] = i;
	                    go_on = false;
	                }
	                z++;
	            }
	        }
		}
		return new_indices;
	}
	//same as getInsertionSortIndices_two_keys, different version not optimized
	function getInsertionSortIndices_two_keys_not_optimized(uint[] memory arr_first, uint[] memory arr_second, bool ascending_first, bool ascending_second) public view returns(uint[] memory) {
	    uint[] memory new_indices = lib.getIndices(arr_first.length);
		uint new_ind;
		
		uint[] memory arr_first_new = lib.copyArray(arr_first, 0, 1);
        uint[] memory arr_second_new = lib.copyArray(arr_first, 0, 1);
		
		for(uint i = 1; i < new_indices.length; i++) {
	        new_ind = lib.findPosition_new_element_sort(i - 1, new_indices[i], arr_first_new, arr_second_new, ascending_first, ascending_second);
            if(new_ind < i) {
                new_indices = lib.slice_elements_arr(new_indices, new_ind, i);
                arr_first_new = lib.reorderArr(new_indices, arr_first, 0, i);
                arr_second_new = lib.reorderArr(new_indices, arr_second, 0, i);
            }
            if(i < arr_first.length - 1) {
                arr_first_new[i + 1] = arr_first[i + 1];
                arr_second_new[i + 1] = arr_second[i + 1];
            }
		}
		return new_indices;
	}
	//using insertionsort, sorts a list of offer_bid by price and then quantity
	function insertionSortOffersBidsPrice_Quantity(Lb.LemLib.offer_bid[] memory offers_bids, bool ascending_price,  bool ascending_quantity) public view returns(Lb.LemLib.offer_bid[] memory) {
		if(offers_bids.length == 0) return offers_bids;
		uint[] memory prices = lib.arr_of_prices_offerbids(offers_bids);
		uint[] memory quantities = lib.arr_of_quantities_offerbids(offers_bids);
		uint[] memory new_indices = getInsertionSortIndices_two_keys(prices, quantities, ascending_price, ascending_quantity);
		return lib.reorderArr_OfferBid(new_indices, offers_bids);
	}
	//using insertionsort, sorts a list of offer_bid by price and then quality. if simulation_test is true, it also performs the sort over the additional key of quantity
	function insertionSortOffersBidsPrice_Quality(Lb.LemLib.offer_bid[] memory offers_bids, bool ascending_price,  bool ascending_quality, bool simulation_test, bool ascending_quantity) public view returns(Lb.LemLib.offer_bid[] memory) {
		if(offers_bids.length == 0) return offers_bids;
		uint[] memory prices = lib.arr_of_prices_offerbids(offers_bids);
		uint[] memory qualities = lib.arr_of_qualities_offerbids(offers_bids);
		uint[] memory new_indices;
		if(simulation_test) {
			uint[] memory quantities = lib.arr_of_quantities_offerbids(offers_bids);
			new_indices = getInsertionSortIndices_three_keys(prices, qualities, quantities, ascending_price, ascending_quality, ascending_quantity);
		}
		else {
			 new_indices = getInsertionSortIndices_two_keys(prices, qualities, ascending_price, ascending_quality);
		}
		return lib.reorderArr_OfferBid(new_indices, offers_bids);
	}
	//aggregate positions with the same user id, same price, and same quality. the quantity of energy is summed up
    function aggregate_identical_positions(Lb.LemLib.offer_bid[] memory offers_bids, bool simulation_test) public view returns (Lb.LemLib.offer_bid[] memory) {
        if(offers_bids.length >= 2) {
            offers_bids = insertionSortOffersBidsPrice_Quality(offers_bids, true, true, simulation_test, true);
            uint count = offers_bids.length;
            for(uint i = 1; i < offers_bids.length; i++) {
                if(lib.compareStrings(offers_bids[i].id_user, offers_bids[i-1].id_user) && offers_bids[i].price_energy == offers_bids[i-1].price_energy && offers_bids[i].quality_energy == offers_bids[i-1].quality_energy) {
                    count--;
                }
            }

            if(count == offers_bids.length) return offers_bids;

            Lb.LemLib.offer_bid[] memory aggregated_offers_bids = new Lb.LemLib.offer_bid[](count);
            aggregated_offers_bids[0] = offers_bids[0];
            uint j = 0;
            for(uint i = 1; i < offers_bids.length; i++) {
                if(lib.compareStrings(offers_bids[i].id_user, offers_bids[i-1].id_user) && offers_bids[i].price_energy == offers_bids[i-1].price_energy && offers_bids[i].quality_energy == offers_bids[i-1].quality_energy) {
                    aggregated_offers_bids[j].qty_energy = aggregated_offers_bids[j].qty_energy + offers_bids[i].qty_energy;
                }
                else {
                    j++;
                    aggregated_offers_bids[j] = offers_bids[i];
                }
            }
            return aggregated_offers_bids;
        }
        return offers_bids;
	}
	// similar function to the get_indices_and_sort_quicksort but instead taking temp_market results as arrays
	function get_market_results_and_sort_quicksort(uint[] memory values, Lb.LemLib.market_result[] memory results, bool ascending) private view returns(Lb.LemLib.market_result[] memory) {
	    uint[] memory indices = new uint[](values.length);
	    for (uint z = 0; z < indices.length; z++) {
            indices[z] = z;
	    }
		Sorting.quickSort_indices(values, 0, int(values.length-1), indices);
		if(!ascending){
		    indices = lib.reverseArray(indices, 0, indices.length - 1);
		}
		Lb.LemLib.market_result[] memory sorted = new Lb.LemLib.market_result[](values.length);
		for (uint z = 0; z < indices.length; z++) {
            sorted[z] = results[indices[z]];
	    }
	    return sorted;
	}
	function quick_sort_market_result_ts_delivery(Lb.LemLib.market_result[] memory arr, bool ascending) public returns(Lb.LemLib.market_result[] memory){
		if(arr.length == 0) return arr;
		uint[] memory ts_deliveries = lib.arr_of_ts_deliveries_market_result(arr);
		Lb.LemLib.market_result[] memory sorted = get_market_results_and_sort_quicksort(ts_deliveries,arr,ascending);
		return sorted;
	}
}