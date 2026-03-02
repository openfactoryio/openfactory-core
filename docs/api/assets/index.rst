OpenFactory Assets
==================

Two classes, :class:`Asset <openfactory.assets.asset_class.Asset>` and :class:`AssetUNS <openfactory.assets.asset_uns_class.AssetUNS>` are available to interact with deployed OpenFactory Assets.
The :class:`Asset <openfactory.assets.asset_class.Asset>` class uses the ``ASSET_UUID`` to identify the Asset,
whereas the :class:`AssetUNS <openfactory.assets.asset_uns_class.AssetUNS>` class uses the UNS (unified namespace) to identify the Asset.

Both classes are derived from the abstract :class:`BaseAsset <openfactory.assets.asset_base.BaseAsset>` class.

.. toctree::
   :maxdepth: 2

   assets_base
   assets
   assets_uns

Below are listed additional classes and methods which help manipualting OpenFactory Assets

.. toctree::
   :maxdepth: 2

   assets_utils
