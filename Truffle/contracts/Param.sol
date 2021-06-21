pragma solidity >=0.5.0 <0.7.5;
pragma experimental ABIEncoderV2;

//basic contract to get parametric data used in the algorithms
contract Param {
    
    string id_supplier = 'supplier01';        //id of energy supplier
    uint qty_energy_supplier_bid = 100000;    //quantity supplier bids on lem
    uint qty_energy_supplier_offer = 100000;  //quantity supplier offers on lem
    uint price_supplier_offer = 80000;         //offer price in sigma/Wh
    uint price_supplier_bid = 20000;     //bid price in sigma/Wh
    uint premium_preference_quality = 0;

	function getIdSupplier() public view returns(string memory) {
	    return id_supplier;
	}
	function getPriceOfferSupplier() public view returns(uint) {
	    return price_supplier_offer;
	}
	function getPriceBidSupplier() public view returns(uint) {
	    return price_supplier_bid;
	}
	function getQtyOfferSupplier() public view returns(uint) {
	    return qty_energy_supplier_offer;
	}
	function getQtyBidSupplier() public view returns(uint) {
	    return qty_energy_supplier_bid;
	}
	function getPremium_preference_quality() public view returns(uint) {
	    return premium_preference_quality;
	}
}