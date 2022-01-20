<?php
/*
 * Copyright (c) 2021, AAChibilyaev
 *
 * Chibilyaev Alexandr <info@aachibilyaev.com>
 *
 * AAChibilyaev LTD <https://aachibilyaev.com/>
 */

namespace aachibilyaev\bxelasticsearch;
use \Tkit\Delivery;

class BxHelpers
{
	public static function getDeliveryData(array $products = [])
	{
		$ids = [];
		foreach($products as $product)
			$ids[] = ['ID' => $product['_id']];

		$obDelivery = new Delivery(array(), date("N"), date("Hi"), false, array("IS_MOSCOW" => \CGeo::isMoscow()));
		$obDelivery->setProducts($ids);
		$DELIVERY_OPTIONS_NO_AVAILABLE = $obDelivery->getDeliveryOptions(true);
		$DELIVERY_OPTIONS = $obDelivery->getDeliveryOptions();
		foreach($products as $key => $product)
		{
			$products[$key]['_source']['DELIVERY_OPTIONS_NO_AVAILABLE'] = $DELIVERY_OPTIONS_NO_AVAILABLE[$product['_id']];
			$products[$key]['_source']['DELIVERY_OPTIONS'] = $DELIVERY_OPTIONS[$product['_id']];
		}
		return $products;
	}
}

