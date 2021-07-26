
var Settlement = artifacts.require("./Settlement.sol");
// JavaScript export
module.exports = function(deployer) {
    // Deployer is the Truffle wrapper for deploying
    // contracts to the network

    // Deploy the contract to the network
   
	deployer.deploy(Settlement);
}