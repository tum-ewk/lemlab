var Clearing = artifacts.require("./ClearingExAnte.sol");
var Param = artifacts.require("./Param.sol");
var Sorting = artifacts.require("./Sorting.sol");
var Lib = artifacts.require("./LemLib.sol");
var Settlement = artifacts.require("./Settlement.sol");
// JavaScript export
module.exports = async function(deployer) {
    // Deployer is the Truffle wrapper for deploying
    // contracts to the network

    // Deploy the contract to the network
    await deployer.deploy(Param);
    await deployer.deploy(Sorting);
    await deployer.deploy(Lib);
    await deployer.deploy(Clearing);
    // we then get the address of the ClearingExAnte contract and pass it to the Settlement contract
    let a = await Clearing.deployed();
    await deployer.deploy(Settlement, a.address);
}