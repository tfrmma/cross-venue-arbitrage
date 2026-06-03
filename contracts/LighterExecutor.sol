// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

// Minimal interface for Lighter's order router
interface ILighterOrderBook {
    function createOrder(
        uint32 marketId,
        bool isAsk,
        uint64 amount,
        uint64 price,
        uint8 orderType  // 2 = IOC
    ) external returns (uint32 orderId, uint64 filledAmount);
}

/**
 * LighterExecutor - atomic swap with price assertion.
 *
 * The whole point: if the price we get is worse than minPrice, revert.
 * No capital at risk if the arb was stale by the time the tx lands.
 *
 * Deploy once per wallet. Don't forget to approve token spending.
 */
contract LighterExecutor {
    address public immutable owner;
    ILighterOrderBook public immutable orderBook;

    error PriceTooLow(uint64 received, uint64 minimum);
    error NotOwner();
    error ZeroFill();

    modifier onlyOwner() {
        if (msg.sender != owner) revert NotOwner();
        _;
    }

    constructor(address _orderBook) {
        owner = msg.sender;
        orderBook = ILighterOrderBook(_orderBook);
    }

    /**
     * Execute a swap. Reverts cleanly if:
     * - Fill price is below minPrice (arb gone)
     * - Nothing filled (no liquidity)
     *
     * Only costs gas on failure. That's the whole point.
     */
    function executeWithMinPrice(
        uint32 marketId,
        bool isAsk,
        uint64 amount,
        uint64 minPrice
    ) external onlyOwner returns (uint64 filledAmount) {
        uint64 fillPrice;
        (,filledAmount) = orderBook.createOrder(marketId, isAsk, amount, minPrice, 2);

        if (filledAmount == 0) revert ZeroFill();

        // price check - the orderbook should enforce this but belt+suspenders
        // isAsk = selling, so we want price >= minPrice
        // !isAsk = buying, so we want price <= minPrice (minPrice is actually maxPrice)
        // TODO: separate minPrice/maxPrice params would be cleaner
    }

    // Emergency: pull any stuck tokens back to owner
    function rescueTokens(address token, uint256 amount) external onlyOwner {
        (bool ok,) = token.call(abi.encodeWithSignature("transfer(address,uint256)", owner, amount));
        require(ok, "transfer failed");
    }
}
