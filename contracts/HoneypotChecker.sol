// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

interface IRouter {
    function swapExactETHForTokensSupportingFeeOnTransferTokens(
        uint256 amountOutMin,
        address[] calldata path,
        address to,
        uint256 deadline
    ) external payable;

    function swapExactTokensForETHSupportingFeeOnTransferTokens(
        uint256 amountIn,
        uint256 amountOutMin,
        address[] calldata path,
        address to,
        uint256 deadline
    ) external;
}

interface IERC20 {
    function approve(address spender, uint256 amount) external returns (bool);
    function balanceOf(address account) external view returns (uint256);
}

contract HoneypotChecker {
    constructor(address router, address wbnb, address token) payable {
        require(msg.value > 0, "NO_VALUE");
        address[] memory path = new address[](2);
        path[0] = wbnb;
        path[1] = token;

        IRouter(router).swapExactETHForTokensSupportingFeeOnTransferTokens{
            value: msg.value
        }(0, path, address(this), block.timestamp + 60);

        uint256 balance = IERC20(token).balanceOf(address(this));
        require(balance > 0, "NO_TOKENS");
        IERC20(token).approve(router, balance);

        address[] memory reverse = new address[](2);
        reverse[0] = token;
        reverse[1] = wbnb;

        IRouter(router).swapExactTokensForETHSupportingFeeOnTransferTokens(
            balance,
            0,
            reverse,
            address(this),
            block.timestamp + 60
        );
    }
}
