var Platform = artifacts.require("./Platform.sol");
var Param = artifacts.require("./Param.sol");
var Sorting = artifacts.require("./Sorting.sol");
var Lib = artifacts.require("./Lib.sol");
// JavaScript export
module.exports = function(deployer) {
    // Deployer is the Truffle wrapper for deploying
    // contracts to the network

    // Deploy the contract to the network
    deployer.deploy(Platform);
    deployer.deploy(Param);
    deployer.deploy(Sorting);
    deployer.deploy(Lib);
}