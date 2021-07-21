var Platform = artifacts.require("./clearing_ex_ante.sol");
var Param = artifacts.require("./param.sol");
var Sorting = artifacts.require("./sorting.sol");
var Lib = artifacts.require("./lem_lib.sol");
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